# frozen_string_literal: true

require 'eventq/worker_status'

module EventQ
  class QueueWorker
    attr_accessor :is_running
    attr_reader :worker_status, :worker_adapter

    def initialize
      @worker_status = EventQ::WorkerStatus.new
      @is_running = false
      @last_gc_flush = Time.now
      @gc_flush_interval = 10
    end

    def start(queue, options = {}, &block)
      EventQ.logger.info("[#{self.class}] - Preparing to start listening for messages.")

      # Make sure mandatory options are specified
      mandatory = [:worker_adapter, :client]
      missing = mandatory - options.keys
      raise "[#{self.class}] - Missing options. #{missing} must be specified." unless missing.empty?

      @worker_adapter = options[:worker_adapter]
      worker_adapter.context = self

      raise "[#{self.class}] - Worker is already running." if running?

      configure(queue, options)
      worker_adapter.configure(options)

      queue_name = EventQ.create_queue_name(queue)
      EventQ.logger.info("[#{self.class}] - Listening for messages on queue: #{queue_name}}")

      # Allow the worker to be started on a thread or on the main process.
      # Using the thread won't block the parent process, whereas starting on the main process will.
      if @block_process
        start_worker(block, options, queue)
      else
        Thread.new { start_worker(block, options, queue) }
      end
      @is_running = true
    end

    def start_worker(block, options, queue)
      if @fork_count > 0
        @fork_count.times do
          pid = fork do
            start_process(options, queue, block)
          end
          # For the parent worker to know about the list of PIDS of the forks, we have to track them after the fork
          # is created. In a fork the collection would be copied and there is no shared reference between processes.
          # So each fork gets its own copy of the @worker_status variable.
          track_process(pid)
        end

        Process.waitall
      else
        # No need to track process/threads separately as we are in the main parent process,
        # and the logic inside start_process will handle it correctly.
        start_process(options, queue, block)
      end
    end

    def start_process(options, queue, block)
      %w'INT TERM'.each do |sig|
        Signal.trap(sig) {
          stop
          exit
        }
      end

      # need to set it again since we might be in a fork.
      @is_running = true
      tracker = track_process(Process.pid)

      # Execute any specific adapter worker logic before the threads are launched.
      # This could range from setting instance variables, extra options, etc.
      worker_adapter.pre_process(self, options)

      if @thread_count > 0
        @thread_count.times do
          thr = Thread.new do
            start_thread(queue, options, block)
          end

          # Allow the thread to kill the parent process if an error occurs
          thr.abort_on_exception = true
          track_thread(tracker, thr)
        end
      else
        start_thread(queue, options, block)
      end

      # Only on the main process should you be able to not wait on a thread, otherwise
      # any forked process will just immediately quit
      unless options[:wait] == false && options[:fork_count] == 0
        worker_status.threads.each { |thr| thr.thread.join }
      end
    end

    def start_thread(queue, options, block)
      worker_adapter.thread_process_iteration(queue, options, block)
    rescue Exception => e # rubocop:disable Lint/RescueException
      EventQ.logger.error(e)
      call_on_error_block(error: e, message: e.message)
      raise Exceptions::WorkerThreadError, e.message, e.backtrace
    end

    # Method to be called by an adapter.  This defines the common logic for processing a message.
    # @param [Array] acceptance_args list of arguments that would be used to accept a message by an adapter.
    # @return [Symbol, MessageArgs] :accepted, :duplicate, :reject
    def process_message(block, message, retry_attempts, acceptance_args)
      abort = false
      kill = false
      error = false
      status = nil

      message_args = EventQ::MessageArgs.new(
          type: message.type,
          retry_attempts: retry_attempts,
          context: message.context,
          content_type: message.content_type,
          id: message.id,
          sent: message.created
      )

      EventQ.logger.debug("[#{self.class}] - Message received. Id: #{message.id}. Retry Attempts: #{retry_attempts}")

      if (!EventQ::NonceManager.is_allowed?(message.id))
        EventQ.logger.warn("[#{self.class}] - Duplicate Message received. Id: #{message.id}. Ignoring message.")
        status = :duplicate
        return status, message_args
      end

      # begin worker block for queue message
      begin
        block.call(message.content, message_args)

        if message_args.abort == true
          abort = true
          EventQ.logger.debug("[#{self.class}] - Message aborted. Id: #{message.id}.")
        elsif message_args.kill == true
          kill = true
          EventQ.logger.debug("[#{self.class}] - Message killed. Id: #{message.id}.")
        else
          # accept the message as processed
          status = :accepted
          worker_adapter.acknowledge_message(*acceptance_args)
          EventQ.logger.debug("[#{self.class}] - Message acknowledged. Id: #{message.id}.")
        end
      rescue => e
        EventQ.logger.error do
          "[#{self.class}] - Unhandled error while attempting to process a queue message. Id: #{message.id}. " \
          "Error: #{e.message} #{e.backtrace.join("\n")}"
        end

        error = true
        call_on_error_block(error: e, message: message)
      end

      if error || abort || kill
        EventQ::NonceManager.failed(message.id)
        status = :reject
      else
        EventQ::NonceManager.complete(message.id)
      end

      [status, message_args]
    end

    def stop
      EventQ.logger.info("[#{self.class}] - Stopping.")
      @is_running = false
      # Need to notify all processes(forks) to stop as well.
      worker_status.pids.each do |pid|
        begin
          Process.kill('TERM', pid) if Process.pid != pid
        rescue Errno::ESRCH
          # Continue on stopping if the process already died and can't be found.
        end
      end
    end

    def running?
      @is_running
    end

    def deserialize_message(payload)
      provider = @serialization_provider_manager.get_provider(EventQ::Configuration.serialization_provider)
      provider.deserialize(payload)
    end

    def serialize_message(msg)
      provider = @serialization_provider_manager.get_provider(EventQ::Configuration.serialization_provider)
      provider.serialize(msg)
    end

    def gc_flush
      if Time.now - last_gc_flush > @gc_flush_interval
        GC.start
        @last_gc_flush = Time.now
      end
    end

    def last_gc_flush
      @last_gc_flush
    end

    def configure(queue, options = {})
      # default thread count
      @thread_count = 1
      if options.key?(:thread_count)
        @thread_count = options[:thread_count] if options[:thread_count] > 0
      end

      # default sleep time in seconds
      @sleep = 0
      if options.key?(:sleep)
        EventQ.logger.warn("[#{self.class}] - :sleep is deprecated.")
      end

      @fork_count = 0
      if options.key?(:fork_count)
        @fork_count = options[:fork_count]
      end

      if options.key?(:gc_flush_interval)
        @gc_flush_interval = options[:gc_flush_interval]
      end

      # The default is to block the process where the worker starts.
      # You may not want it to block if an application needs to run multiple things at the same time.
      # Example:  Running a background worker and a web service on the same application.
      @block_process = true
      if options.key?(:block_process)
        @block_process = options[:block_process]
      end

      message_list = [
          "Process Count: #{@fork_count}",
          "Thread Count: #{@thread_count}",
          "Interval Sleep: #{@sleep}",
          "GC Flush Interval: #{@gc_flush_interval}",
          "Block process: #{@block_process}"
      ]
      EventQ.logger.info("[#{self.class}] - Configuring. #{message_list.join(' | ')}")
    end

    def on_retry_exceeded(&block)
      @on_retry_exceeded_block = block
    end

    def on_killed(&block)
      @on_killed_block = block
    end

    def on_retry(&block)
      @on_retry_block = block
    end

    def on_error(&block)
      @on_error_block = block
    end

    def call_on_error_block(error:, message: nil)
      call_block(:on_error_block, error, message)
    end

    def call_on_retry_exceeded_block(message)
      call_block(:on_retry_exceeded_block, message)
    end

    def call_on_killed_block(message)
      call_block(:on_killed_block, message)
    end

    def call_on_retry_block(message)
      call_block(:on_retry_block, message)
    end

    private

    def call_block(block_name, *args)
      block_variable = "@#{block_name}"
      if instance_variable_get(block_variable)
        EventQ.logger.debug { "[#{self.class}] - Executing #{block_variable}." }
        begin
          instance_variable_get(block_variable).call(*args)
        rescue => e
          EventQ.logger.error("[#{self.class}] - An error occurred executing the #{block_variable}. Error: #{e}")
        end
      else
        EventQ.logger.debug { "[#{self.class}] - No #{block_variable} specified." }
      end
    end

    def track_process(pid)
      tracker = EventQ::WorkerProcess.new(pid)
      worker_status.processes.push(tracker)
      tracker
    end

    def track_thread(process_tracker, thread)
      tracker = EventQ::WorkerThread.new(thread)
      process_tracker.threads.push(tracker)
      tracker
    end
  end
end
