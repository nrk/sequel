require 'System.Data'
Sequel.require 'adapters/shared/mssql'
include System::Data
include System::Data::SqlClient

class String
    def to_clr_string_utf8
        System::Text::Encoding.UTF8.GetString(self.ToByteArray)
    end
end

class System::String
    def to_s_utf8
        String.new(System::Text::Encoding.UTF8.GetBytes(self))
    end
end

class System::DBNull
    def to_str; ''; end
end

class SqlConnection
    def execute(sql, mode = :execute_non_query)
        # TODO: command timeout?
        cmd = SqlCommand.new(sql, self)
        cmd.command_type = CommandType.Text
        cmd.send(mode)
    end
end

class SqlDataReader
    include Enumerable
    SYSTEM_STRING = System::String.to_clr_type
    attr_accessor :utf8

    def eof?
        is_closed
    end

    def fields
        fields = []
        to_s = @utf8 ? :to_s_utf8 : :to_s
        0.upto(field_count - 1) do |field_index|
            fields << get_name(field_index).send(to_s)
        end if field_count > 0
        fields
    end

    def each
        begin
            to_s = @utf8 ? :to_s_utf8 : :to_s
            while self.read
                row = []
                0.upto(field_count - 1) do |field_index|
                    value = get_value(field_index)
                    row << (is_clr_string(value) ? value.send(to_s) : value)
                end if field_count > 0
                yield row
            end
        ensure
            close
        end
    end

    def rows
        inject([]) { |rows,row| rows << row }
    end

    private
    def is_clr_string(value)
        value.class.to_clr_type == SYSTEM_STRING
    end
end

module Sequel
    module MSSQLClient
        class Database < Sequel::Database
            include ::Sequel::MSSQL::DatabaseMethods

            set_adapter_scheme :mssqlclient
            attr_reader :utf8

            def connect(server)
                opts   = server_opts(server)
                @utf8  = opts[:utf8]
                handle = SqlConnection.new
                handle.connection_string = opts[:conn_string] || build_connection_string(opts)
                handle.open
                handle
            end

            def server_version(server=nil)
                return @server_version if @server_version
                @server_version = synchronize(server) do |conn|
                    m = SERVER_VERSION_RE.match(conn.server_version.to_s)
                    (m[1].to_i * 1000000) + (m[2].to_i * 10000) + m[3].to_i
                end
                @server_version
            end

            def dataset(opts = nil)
                MSSQLClient::Dataset.new(self, opts)
            end
    
            def execute(sql, opts={})
                log_info(sql)
                synchronize(opts[:server]) do |conn|
                    begin
                        cmd = create_command(sql, conn, opts)
                        r = cmd.execute_reader
                        r.utf8 = @utf8
                        yield(r) if block_given?
                    rescue ::Exception => e
                        raise_error(e)
                    ensure
                        r.close unless r.nil? or r.eof?
                    end
                end
                nil
            end
            alias do execute

            def single_value(sql, opts={})
                log_info(sql)
                synchronize(opts[:server]) do |conn|
                    begin
                        cmd = create_command(sql, conn, opts)
                        r = cmd.execute_scalar
                        yield(r) if block_given?
                    rescue ::Exception => e
                        raise_error(e)
                    end
                end
                nil
            end

            private
            def build_connection_string(opts)
                connstr = "Data Source=#{opts[:host]};Initial Catalog=#{opts[:database]};"
                connstr << "User ID=#{opts[:user]};Password=#{opts[:password]}" if opts[:user]
            end

            def create_command(sql, conn, opts={})
                cmd = SqlCommand.new(sql.send(@utf8 ? :to_clr_string_utf8 : :to_clr_string), conn)
                cmd.command_timeout = opts[:command_timeout] if opts[:command_timeout]
                cmd.command_type = CommandType.Text
                cmd
            end

            def disconnect_connection(conn)
                conn.close
            end
        end

        class Dataset < Sequel::Dataset
            include ::Sequel::MSSQL::DatasetMethods

            def fetch_rows(sql)
                execute(sql) do |s|
                    @columns = cols = s.fields.map{|column| output_identifier(column)}
                    s.each do |values|
                        row = {}
                        cols.each_with_index { |n,i| row[n] = values[i] }
                        yield row
                    end
                end
            end
        end
    end
end