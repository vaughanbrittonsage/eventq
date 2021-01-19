require 'spec_helper'


RSpec.describe EventQ::RabbitMq::QueueWorker do
  let(:queue_worker) { EventQ::QueueWorker.new }

  let(:client) { EventQ::RabbitMq::QueueClient.new({ endpoint: 'rabbitmq' }) }

  let(:connection) { client.get_connection }

  let(:channel) { connection.create_channel }

  after do
    begin
    channel.close if channel.open?
    connection.close if connection.open?
    rescue => e
      EventQ.logger.error { "Timeout error occurred closing connection. Error: #{e}" }
    end
  end

  describe '#deserialize_message' do
    context 'when serialization provider is OJ_PROVIDER' do
      before do
        EventQ::Configuration.serialization_provider = EventQ::SerializationProviders::OJ_PROVIDER
      end
      context 'when payload is for a known type' do
        let(:a) do
          A.new.tap do |a|
            a.text = 'ABC'
          end
        end
        let(:payload) { Oj.dump(a) }
        it 'should deserialize the message into an object of the known type' do
          message = subject.deserialize_message(payload)
          expect(message).to be_a(A)
          expect(message.text).to eq('ABC')
        end
      end
      context 'when payload is for an unknown type' do
        let(:a) do
          A.new.tap do |a|
            a.text = 'ABC'
          end
        end
        let(:payload) do
          string = Oj.dump(a)
          JSON.load(string.sub('"^o":"A"', '"^o":"B"'))
        end
        let(:message) do
          EventQ::QueueMessage.new.tap do |m|
            m.content = payload
          end
        end
        let(:json) do
          Oj.dump(message)
        end
        it 'should deserialize the message into a Hash' do
          message = subject.deserialize_message(json)
          expect(message.content).to be_a(Hash)
          expect(message.content[:text]).to eq('ABC')
        end
      end
    end
    context 'when serialization provider is JSON_PROVIDER' do
      before do
        EventQ::Configuration.serialization_provider = EventQ::SerializationProviders::JSON_PROVIDER
      end
      let(:payload) do
        {
            content: { text: 'ABC' }
        }
      end
      let(:json) do
        JSON.dump(payload)
      end
      it 'should deserialize payload' do
        message = subject.deserialize_message(json)
        expect(message).to be_a(EventQ::QueueMessage)
        expect(message.content).to be_a(Hash)
        expect(message.content[:text]).to eq('ABC')
      end

      after do
        EventQ::Configuration.serialization_provider = EventQ::SerializationProviders::OJ_PROVIDER
      end
    end
  end

  describe '#gc_flush' do
    context 'when the last gc_flush was made more than the gc_flush_interval length ago' do
      it 'should execute a GC flush' do
        allow(queue_worker).to receive(:last_gc_flush).and_return(Time.now - 15)
        expect(GC).to receive(:start).once
        queue_worker.gc_flush
      end
    end
    context 'when the last gc_flush was made NOT more than the gc_flush_interval length ago' do
      it 'should NOT execute a GC flush' do
        allow(queue_worker).to receive(:last_gc_flush).and_return(Time.now - 5)
        expect(GC).not_to receive(:start)
        queue_worker.gc_flush
      end
    end
  end

  context 'NonceManager' do
    context 'when a duplicate message is received' do
      let(:queue_message) { EventQ::QueueMessage.new }

      before do
        EventQ::NonceManager.configure(server: 'redis://redis:6379')
      end

      it 'should NOT process the message more than once' do
        event_type = SecureRandom.uuid
        subscriber_queue = EventQ::Queue.new
        subscriber_queue.name = SecureRandom.uuid
        #set queue retry delay to 0.5 seconds
        subscriber_queue.retry_delay = 500
        subscriber_queue.allow_retry = true

        qm = EventQ::RabbitMq::QueueManager.new
        q = qm.get_queue(channel, subscriber_queue)
        q.delete

        subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
        subscription_manager.subscribe(event_type, subscriber_queue)

        message = 'Hello World'

        eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})

        allow(eqclient).to receive(:new_message).and_return(queue_message)

        eqclient.raise_event(event_type, message)
        eqclient.raise_event(event_type, message)

        queue_worker.configure(sleep: 0)

        received_count = 0

        queue_worker.start(subscriber_queue, { worker_adapter: subject, client: client, wait: false, block_process: false, sleep: 0, thread_count: 1 }) do |content, args|
          received_count += 1
        end

        sleep(2)

        queue_worker.stop

        expect(received_count).to eq 1
      end

      after do
        EventQ::NonceManager.reset
      end
    end
  end

  describe '#call_on_error_block' do
    let(:error) { double }
    let(:message) { double }

    context 'when a block is specified' do
      let(:block) { Proc.new { } }
      before do
        queue_worker.on_error &block
        allow(block).to receive(:call)
      end
      it 'should execute the block' do
        expect(block).to receive(:call).with(error, message).once
        queue_worker.call_on_error_block(error: error, message: message)
      end
    end

    context 'when a block is NOT specified' do
      let(:block) { nil }
      before do
        queue_worker.on_error &block
      end
      it 'should NOT execute the block' do
        expect(block).not_to receive(:call)
        queue_worker.call_on_error_block(error: error, message: message)
      end
    end
  end

  describe '#call_on_retry_block' do
    let(:message) { double }
    context 'when a block is specified' do
      let(:block) { Proc.new { } }
      before do
        queue_worker.on_retry &block
        allow(block).to receive(:call)
      end
      it 'should execute the block' do
        expect(block).to receive(:call).with(message).once
        queue_worker.call_on_retry_block(message)
      end
    end
    context 'when a block is NOT specified' do
      let(:block) { nil }
      before do
        queue_worker.on_retry &block
      end
      it 'should NOT execute the block' do
        expect(block).not_to receive(:call)
        queue_worker.call_on_retry_block(message)
      end
    end
  end

  describe '#call_on_retry_exceeded_block' do
    let(:message) { double }
    context 'when a block is specified' do
      let(:block) { Proc.new { } }
      before do
        queue_worker.on_retry_exceeded &block
        allow(block).to receive(:call)
      end
      it 'should execute the block' do
        expect(block).to receive(:call).with(message).once
        queue_worker.call_on_retry_exceeded_block(message)
      end
    end
    context 'when a block is NOT specified' do
      let(:block) { nil }
      before do
        queue_worker.on_retry_exceeded &block
      end
      it 'should NOT execute the block' do
        expect(block).not_to receive(:call)
        queue_worker.call_on_retry_exceeded_block(message)
      end
    end
  end

  describe '#call_on_killed_block' do
    let(:message) { double }
    context 'when a block is specified' do
      let(:block) { Proc.new { } }
      before do
        queue_worker.on_killed &block
        allow(block).to receive(:call)
      end
      it 'should execute the block' do
        expect(block).to receive(:call).with(message).once
        queue_worker.call_on_killed_block(message)
      end
    end
    context 'when a block is NOT specified' do
      let(:block) { nil }
      before do
        queue_worker.on_killed &block
      end
      it 'should NOT execute the block' do
        expect(block).not_to receive(:call)
        queue_worker.call_on_killed_block(message)
      end
    end
  end

  it 'should receive an event from the subscriber queue' do

    event_type = 'queue.worker.event1'
    subscriber_queue = EventQ::Queue.new
    subscriber_queue.name = 'queue.worker1'

    subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({ client: client})
    subscription_manager.subscribe(event_type, subscriber_queue)

    message = 'Hello World'
    message_context = { 'foo' => 'bar' }

    eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
    eqclient.raise_event(event_type, message, message_context)

    queue_worker.start(subscriber_queue, {worker_adapter: subject, wait: false, block_process: false, :sleep => 1, client: client, thread_count: 1 }) do |event, args|
      expect(event).to eq(message)
      expect(args.type).to eq(event_type)
      expect(args.content_type).to eq message.class.to_s
      expect(args.context).to eq message_context
      EventQ.logger.debug { "Message Received: #{event}" }
    end

    sleep(1)

    queue_worker.stop

    expect(queue_worker.running?).to eq(false)
  end

  context 'when queue requires a signature' do
    let(:secret) { 'secret' }
    before do
      EventQ::Configuration.signature_secret = secret
    end
    context 'and the received message contains a valid signature' do
      it 'should process the message' do

        event_type = SecureRandom.uuid
        subscriber_queue = EventQ::Queue.new
        subscriber_queue.name = SecureRandom.uuid
        subscriber_queue.require_signature = true

        subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({ client: client})
        subscription_manager.subscribe(event_type, subscriber_queue)

        message = 'Hello World'

        eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
        eqclient.raise_event(event_type, message)

        received = false

        queue_worker.start(subscriber_queue, {worker_adapter: subject, wait: false, block_process: false, :sleep => 1, client: client}) do |event, args|
          expect(event).to eq(message)
          expect(args.type).to eq(event_type)
          received = true
          EventQ.logger.debug { "Message Received: #{event}" }
        end

        sleep(1)

        expect(received).to eq(true)

        queue_worker.stop

        expect(queue_worker.running?).to eq(false)

      end
    end
    context 'and the received message contains an invalid signature' do
      it 'should NOT process the message' do

        event_type = SecureRandom.uuid
        subscriber_queue = EventQ::Queue.new
        subscriber_queue.name = SecureRandom.uuid
        subscriber_queue.require_signature = true

        EventQ::Configuration.signature_secret = 'invalid'

        subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({ client: client})
        subscription_manager.subscribe(event_type, subscriber_queue)

        message = 'Hello World'

        eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
        eqclient.raise_event(event_type, message)

        EventQ::Configuration.signature_secret = secret

        received = false

        queue_worker.start(subscriber_queue, {worker_adapter: subject, wait: false, block_process: false, :sleep => 1, client: client}) do |event, args|
          expect(event).to eq(message)
          expect(args.type).to eq(event_type)
          received = true
          EventQ.logger.debug { "Message Received: #{event}" }
        end

        sleep(0.5)

        expect(received).to eq(false)

        queue_worker.stop

        expect(queue_worker.running?).to eq(false)

      end
    end
  end

  it 'should receive events in parallel on each thread from the subscriber queue' do

    event_type = SecureRandom.uuid
    subscriber_queue = EventQ::Queue.new
    subscriber_queue.name = SecureRandom.uuid

    subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
    subscription_manager.subscribe(event_type, subscriber_queue)

    message = 'Hello World'

    received_messages = []

    message_count = 0

    mutex = Mutex.new

    queue_worker.start(subscriber_queue, worker_adapter: subject, wait: false, block_process: false, :sleep => 0.5, :thread_count => 5, client: client) do |event, args|
      expect(event).to eq(message)
      expect(args.type).to eq(event_type)

      mutex.synchronize do
        EventQ.logger.debug { "Message Received: #{event}" }
        message_count += 1
        add_to_received_list(received_messages)
        EventQ.logger.debug { 'message processed.' }
        sleep 0.2
      end
    end

    eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)
    eqclient.raise_event(event_type, message)

    sleep(8)

    expect(message_count).to eq(10)
    expect(received_messages.length).to eq(5)
    expect(received_messages[0][:events]).to be >= 1
    expect(received_messages[1][:events]).to be >= 1
    expect(received_messages[2][:events]).to be >= 1
    expect(received_messages[3][:events]).to be >= 1
    expect(received_messages[4][:events]).to be >= 1

    queue_worker.stop

    expect(queue_worker.running?).to eq(false)

  end

  it 'should send messages that fail to process to the retry queue and then receive them again after the retry delay' do

    event_type = SecureRandom.uuid
    subscriber_queue = EventQ::Queue.new
    subscriber_queue.name = SecureRandom.uuid
    #set queue retry delay to 0.5 seconds
    subscriber_queue.retry_delay = 500
    subscriber_queue.allow_retry = true

    qm = EventQ::RabbitMq::QueueManager.new
    q = qm.get_queue(channel, subscriber_queue)
    q.delete

    subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
    subscription_manager.subscribe(event_type, subscriber_queue)

    message = 'Hello World'

    eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
    eqclient.raise_event(event_type, message)

    retry_attempt_count = 0

    queue_worker.start(subscriber_queue, {worker_adapter: subject, wait: false, block_process: false, :thread_count => 1, :sleep => 0.5, client: client}) do |event, args|

      if args.retry_attempts == 0
        raise 'Fail on purpose to send event to retry queue.'
      end

      retry_attempt_count = args.retry_attempts

    end

    sleep(5)

    expect(retry_attempt_count).to eq(1)

    queue_worker.stop

    expect(queue_worker.running?).to eq(false)

  end

  context 'queue.allow_retry_back_off = true' do
    it 'should send messages that fail to process to the retry queue and then receive them again after the retry delay' do

      event_type = SecureRandom.uuid
      subscriber_queue = EventQ::Queue.new
      subscriber_queue.name = SecureRandom.uuid
      #set queue retry delay to 0.5 seconds
      subscriber_queue.retry_delay = 500
      subscriber_queue.allow_retry = true
      subscriber_queue.allow_retry_back_off = true
      #set to max retry delay to 5 seconds
      subscriber_queue.max_retry_delay = 5000

      qm = EventQ::RabbitMq::QueueManager.new
      q = qm.get_queue(channel, subscriber_queue)
      q.delete

      subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
      subscription_manager.subscribe(event_type, subscriber_queue)

      message = 'Hello World'

      eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
      eqclient.raise_event(event_type, message)

      retry_attempt_count = 0

      queue_worker.start(subscriber_queue, { worker_adapter: subject, wait: false, block_process: false, :thread_count => 1, :sleep => 0.5, client: client}) do |event, args|
        retry_attempt_count = args.retry_attempts
        raise 'Fail on purpose to send event to retry queue.'
      end

      sleep(0.8)

      expect(retry_attempt_count).to eq(1)

      sleep(1.3)

      expect(retry_attempt_count).to eq(2)

      sleep(1.8)

      expect(retry_attempt_count).to eq(3)

      sleep(2.3)

      expect(retry_attempt_count).to eq(4)

      queue_worker.stop

      expect(queue_worker.running?).to eq(false)

    end
  end

  context 'retry block execution' do
    xit 'should execute the #on_retry_exceeded block when a message exceeds its retry limit' do

      event_type = SecureRandom.uuid
      subscriber_queue = EventQ::Queue.new
      subscriber_queue.name = SecureRandom.uuid
      #set queue retry delay to 0.5 seconds
      subscriber_queue.retry_delay = 500
      subscriber_queue.allow_retry = true
      subscriber_queue.max_retry_attempts = 1

      qm = EventQ::RabbitMq::QueueManager.new
      q = qm.get_queue(channel, subscriber_queue)
      q.delete

      subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
      subscription_manager.subscribe(event_type, subscriber_queue)

      message = 'Hello World'

      eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
      eqclient.raise_event(event_type, message)

      retry_attempt_count = 0

      failed_message = nil

      subject.on_retry_exceeded do |message|
        failed_message = message
      end

      queue_worker.start(subscriber_queue, { worker_adapter: subject, wait: false, block_process: false, :thread_count => 1, :sleep => 0.5, client: client }) do |event, args|

        retry_attempt_count = args.retry_attempts
        raise 'Fail on purpose to send event to retry queue.'

      end

      sleep(5)

      expect(retry_attempt_count).to eq(1)
      expect(failed_message.content).to eq(message)
      expect(failed_message.retry_attempts).to eq(1)
      expect(failed_message.type).to eq(event_type)

      queue_worker.stop

      expect(queue_worker.running?).to eq(false)

    end

    xit 'should execute the #on_retry block when a message is retried' do

      event_type = SecureRandom.uuid
      subscriber_queue = EventQ::Queue.new
      subscriber_queue.name = SecureRandom.uuid
      #set queue retry delay to 0.5 seconds
      subscriber_queue.retry_delay = 500
      subscriber_queue.allow_retry = true
      subscriber_queue.max_retry_attempts = 1

      qm = EventQ::RabbitMq::QueueManager.new
      q = qm.get_queue(channel, subscriber_queue)
      q.delete

      subscription_manager = EventQ::RabbitMq::SubscriptionManager.new({client: client})
      subscription_manager.subscribe(event_type, subscriber_queue)

      message = 'Hello World'

      eqclient = EventQ::RabbitMq::EventQClient.new({client: client, subscription_manager: subscription_manager})
      eqclient.raise_event(event_type, message)

      retry_attempt_count = 0

      failed_message = nil
      is_abort = false

      subject.on_retry do |message, abort|
        failed_message = message
        is_abort = abort
      end

      queue_worker.start(subscriber_queue, { worker_adapter: subject, wait: false, block_process: false, :thread_count => 1, :sleep => 0.5, client: client }) do |event, args|

        retry_attempt_count = args.retry_attempts
        raise 'Fail on purpose to send event to retry queue.'

      end

      sleep(1)

      queue_worker.stop

      expect(retry_attempt_count).to eq(1)
      expect(failed_message.content).to eq(message)
      expect(failed_message.retry_attempts).to eq(1)
      expect(failed_message.type).to eq(event_type)

      expect(queue_worker.running?).to eq(false)

    end
  end

  class A
    attr_accessor :text
  end

  def add_to_received_list(received_messages)

    thread_name = Thread.current.object_id
    EventQ.logger.debug { "[THREAD] #{thread_name}" }
    thread = received_messages.select { |i| i[:thread] == thread_name }

    if thread.length > 0
      thread[0][:events] += 1
    else
      received_messages.push({ :events => 1, :thread => thread_name })
    end

  end
end

