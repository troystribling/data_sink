require 'aws-sdk'
require 'json'
require 'date'

module Tweets

  def download_from_s3(start_date: nil, end_date: nil, bucket_name: 'gly.fish', local_dir: '/tmp/tweets', remote_dir: 'tweets/public_stream')
    default_date = Date.today.strftime('%Y%m%d')

    start_date = Date.parse(start_date || default_date)
    end_date = Date.parse(end_date || default_date)

    FileUtils.mkdir_p(local_dir) unless File.directory?(local_dir)

    puts "Downloading #{limit || '∞'} files with date between #{start_date}..#{end_date} from bucket '#{bucket_name}' to '#{local_dir}''"

    s3 = Aws::S3::Resource.new
    bucket = s3.bucket(bucket_name)

    (start_date..end_date).each do |date|
      remote_tweets_folder = "#{remote_dir}/#{date.strftime('%Y%m%d')}"
      remote_files = bucket.objects(prefix: remote_tweets_folder)
      remote_files.each do |remote_file|
        local_file_path = File.join([local_dir, "#{File.basename(remote_file.key)}"])
        File.open(local_file_path, 'wb') do |local_file|
          local_file.write(remote_file.get.body.read)
        end
        `lzop -d #{local_file_path}`
        File.delete(local_file_path)
      end
    end

    puts "Wrote #{count} files to #{local_dir}"
  end
  module_function :download_tweets

  def read_from_file(file_path)
    tweets = []
    File.open(file_path, 'r').read.each_line do |tweet|
      tweets.push(JSON.parse(tweet))
    end
    tweets
  end
  module_function :read_tweets_from_file

end
