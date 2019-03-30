require "mysqldump/version"

class Mysqldump
  class << self
    delegate :dump, :restore, to: :new
  end

  attr_reader :config, :output_filename

  def initialize
    @config = ActiveRecord::Base.connection_config
    @output_filename ||= []
  end

  def dump *tables_or_classes, where: nil
    tables(tables_or_classes, true).map do |x|
      table = x.last
      system "#{dump_cmd table, where: where} | bzip2 - - > #{output_file table}"
      x.first
    end
  end

  def restore *tables_or_classes, db: nil
    tables(tables_or_classes).map { |x|
      table = x.last
      file = latest_file table, db: db
      next unless file
      unzip_file = File.join(File.dirname(file), File.basename(file, '.bz2'))

      system "bunzip2 -k -f #{file} && #{restore_cmd} < #{unzip_file}"
      x.first
    }.compact
  end

  def tables tables_or_classes, filter = false
    tables = Array.wrap(tables_or_classes).map { |x|
      if x.respond_to?(:table_name)
        if !filter || x.count > 0
          [x, x.table_name]
        end
      else
        [x.to_s]
      end
    }.compact
  end

  def credentials
    username = config[:username] || config[:user]

    [].tap { |x|
      x << "-u #{username}" if username
      x << "-p'#{config[:password]}'" if config[:password]
      x << "-h #{config[:host]}" if config[:host]
      x << "-S#{config[:socket]}" if config[:socket]
    }.join ' '
  end

  def database
    config[:database]
  end

  def current_time
    Time.zone.now.strftime("%Y-%m-%d-%H%M%S")
  end

  def output_file table = nil
    fname = [
      database,
      table,
      current_time,
    ].compact.join '_'

    @output_filename << "tmp/#{fname}.sql.bz2"
    @output_filename.last
  end

  def dump_cmd *tables, where: nil
    cmd = [
      "mysqldump",
      credentials,
      database,
      tables,
      "--lock-tables=false"
    ]
    cmd << "\"--where=#{where}\"" if where

    cmd.compact.flatten.join ' '
  end

  def restore_cmd *_tables
    "rails db -p"
  end

  def database_candidates
    candidates = [database]
    parts = database.split "_"

    stages = ["production", "development"]
    if stages.include? parts.last
      candidates += (stages - [parts.last]).map {|x| [parts[0...-1], x].join "_" }
    end

    candidates
  end

  def latest_file table = nil, db: nil
    candidates = db ? [db] : database_candidates

    files = candidates.flat_map do |x|
      fname = [
        x,
        table,
        '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9].sql.bz2',
      ].compact.join '_'

      Dir.glob("tmp/#{fname}")
    end

    files.max_by { |x| x.split("_").last }
  end
end
