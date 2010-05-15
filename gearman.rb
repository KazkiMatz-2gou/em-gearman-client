require File.dirname(__FILE__) + '/gearman/task.rb'
require File.dirname(__FILE__) + '/gearman/util.rb'

module EventMachine
  module Protocols
    # == Usage example
    #
    #   EM.run{
    #     gearman = EM::P::Gearman.connect 'localhost', 7003
    #     task = gearman.create_task('sleep', 5)
    #     task.timeout = 60
    #     task.priority = :high
    #     task.background = false
    #     task.on_complete = Proc{|res| p res}
    #     task.queue!
    #   }
    #
    #  priority -> (:high | :low) the priority of the job, a high priority job is executed before a low on
    #  background -> (true | false) a background task will return no further information to the client.

    module Gearman
      include EM::Deferrable

      ##
      # errors
      class ProtocolError < StandardError;end
      class TimeoutError < StandardError;end

      # Connect to a gearman server
      def self.connect host = 'localhost', port = DEFAULT_PORT
        EM.connect host, port, self, host, port
      end

      ##
      # em hooks

      def initialize host, port = DEFAULT_PORT
        @host, @port = host, port
        @tasks_to_send = []
        @task_waiting_for_handle = nil
      end

      def connection_completed
        #@queue_cbs = []
        #@values = {}
        @tasks_in_progress = {}  # [handle1, handle2, ...]
        @finished_tasks = []  # tasks that have completed or failed

        @reconnecting = false
        @connected = true
        succeed
        # set_delimiter "\r\n"
        # set_line_mode
      end

      def create_task(func, arg)
        Task.new(self, func, arg)
      end

      def add_task task, reset_state = true
        task.reset_state if reset_state
        @tasks_to_send << task
      end

      def send_req
        return false if @tasks_to_send.empty?
        return false if @task_waiting_for_handle

        @task_waiting_for_handle = @tasks_to_send.shift
        req = @task_waiting_for_handle.get_submit_packet()
        callback{
          send_data req
        }
        true
      end


      # >>>>>>>> borrowed from xing-gearman-ruby

      #--
      # 19Feb09 Switched to a custom parser, LineText2 is recursive and can cause
      #         stack overflows when there is too much data.
      # include EM::P::LineText2
      def receive_data data
        Util.logger.debug "GearmanRuby: receive_data called with a packet of #{data.length} bytes"
        (@buffer||='') << data

        while @buffer.length >= 12
          magic, type_int, len = @buffer[0,12].unpack('a4NN')
          type = Util::COMMANDS[type_int]
          raise ProtocolError, "Invalid magic '#{magic}'" unless magic == "\0RES"
          raise ProtocolError, "Invalid packet type #{type}" unless type
          if @buffer.length >= 12 + len
            body = @buffer.slice!(0, 12 + len)
            buf = len > 0 ? body[12, len] : ''

            known_types = [ :job_created,
                            :work_complete,
                            :work_fail,
                            :work_status,
                            :work_exception,
                            :work_warning,
                            :work_data ]

            if known_types.include?(type)
              send("handle_#{type}".to_sym, buf)
            else
              raise ProtocolError, "Unknown type: Got #{type.to_s}"
            end
          else
            return
          end
        end
      end

      ##
      # Handle a 'job_created' response from a job server.
      #
      # @param hostport  "host:port" of job server
      # @param data      data returned in packet from server
      def handle_job_created(data)
        Util.logger.debug "GearmanRuby: Got job_created with handle #{data}"
        if not @task_waiting_for_handle
          raise ProtocolError, "Got unexpected job_created notification " + "with handle #{data}"
        end
        js_handle = Util.handle_to_str(data)
        task = @task_waiting_for_handle
        @task_waiting_for_handle = nil
        if(task.background)
          @finished_tasks << task
        else
          (@tasks_in_progress[js_handle] ||= []) << task
        end
        send_req
        nil
      end
      private :handle_job_created
    
      ##
      # Handle a 'work_complete' response from a job server.
      #
      # @param hostport  "host:port" of job server
      # @param data      data returned in packet from server
      def handle_work_complete(data)
        handle, data = data.split("\0", 2)
        Util.logger.debug "GearmanRuby: Got work_complete with handle #{handle} and #{data ? data.size : '0'} byte(s) of data"
        tasks_in_progress(handle, true).each do |t|
          t.handle_completion(data)
          @finished_tasks << t
        end
        nil
      end
      private :handle_work_complete
    
      ##
      # Handle a 'work_exception' response from a job server.
      #
      # @param hostport  "host:port" of job server
      # @param data      data returned in packet from server
      def handle_work_exception(data)
        handle, exception = data.split("\0", 2)
        Util.logger.debug "GearmanRuby: Got work_exception with handle #{handle} '#{exception}'"
        tasks_in_progress(handle).each {|t| t.handle_exception(exception) }
      end
      private :handle_work_exception
    
      ##
      # Handle a 'work_fail' response from a job server.
      #
      # @param hostport  "host:port" of job server
      # @param data      data returned in packet from server
      def handle_work_fail(data)
        Util.logger.debug "GearmanRuby: Got work_fail with handle #{data}"
        tasks_in_progress(data, true).each do |t|
          if t.handle_failure
            add_task(t, false)
          else
            @finished_tasks << t
          end
        end
      end
      private :handle_work_fail
    
      ##
      # Handle a 'work_status' response from a job server.
      #
      # @param hostport  "host:port" of job server
      # @param data      data returned in packet from server
      def handle_work_status(data)
        handle, num, den = data.split("\0", 3)
        Util.logger.debug "GearmanRuby: Got work_status with handle #{handle} : #{num}/#{den}"
        tasks_in_progress(handle).each {|t| t.handle_status(num, den) }
      end
      private :handle_work_status
    
      ##
      # Handle a 'work_warning' response from a job server.
      #
      # @param hostport "host:port" of job server
      # @param data     data returned in packet from server
      def handle_work_warning(data)
        handle, message = data.split("\0", 2)
        Util.logger.debug "GearmanRuby: Got work_warning with handle #{handle} : '#{message}'"
        tasks_in_progress(handle).each {|t| t.handle_warning(message) }
      end
      private :handle_work_warning
    
      ##
      # Handle a 'work_data' response from a job server
      #
      # @param hostport  "host:port" of a job server
      # @param data       data returned in packet from server
      def handle_work_data(data)
        handle, data = data.split("\0", 2)
        Util.logger.debug "GearmanRuby: Got work_data with handle #{handle} and #{data ? data.size : '0'} byte(s) of data"
    
        js_handle = Util.handle_to_str(handle)
        tasks = @tasks_in_progress[js_handle]
        if not tasks
          raise ProtocolError, "Got unexpected work_data with handle #{handle} (no task by that name)"
        end
        tasks.each {|t| t.handle_data(data) }
      end
      private :handle_work_data

      def tasks_in_progress(handle, remove_task = false)
        js_handle = Util.handle_to_str(handle)
        tasks = remove_task ? @tasks_in_progress.delete(js_handle) : @tasks_in_progress[js_handle]
        if not tasks
          raise ProtocolError, "Got unexpected work_data with handle #{handle} (no task by that name)"
        end
        tasks
      end
      private :tasks_in_progress


      # <<<<<<<<<<<<<<<<<

      def unbind
        Util.logger.debug "GearmanRuby: connection closed."
=begin
        if @connected or @reconnecting
          EM.add_timer(1){ reconnect @host, @port }
          @connected = false
          @reconnecting = true
          @deferred_status = nil
        else
          raise 'Unable to connect to gearman server'
        end
=end
      end

    end
  end
end

