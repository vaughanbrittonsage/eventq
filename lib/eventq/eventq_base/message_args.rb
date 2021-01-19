module EventQ
  class MessageArgs
    attr_reader :type
    attr_reader :content_type
    attr_reader :retry_attempts
    attr_accessor :abort
    attr_accessor :drop
    attr_accessor :kill
    attr_reader :context
    attr_reader :id
    attr_reader :sent

    def initialize(type:, retry_attempts:, context: {}, content_type:, id: nil, sent: nil)
      @type = type
      @retry_attempts = retry_attempts
      @abort = false
      @drop = false
      @kill = false
      @context = context
      @content_type = content_type
      @id = id
      @sent = sent
    end
  end
end
