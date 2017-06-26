require 'aws-sdk'
require 'json'
require 'date'

module Tweets

  def download_tweets(start_date: nil, end_date: nil, limit: nil, bucket_name: 'gly.fish', local_dir: '/tmp/tweets')
    count = 0
    default_date = Date.today.strftime('%Y%m%d')

    start_date = Date.parse(start_date || default_date)
    end_date = Date.parse(end_date || default_date)

    FileUtils.mkdir_p(local_dir) unless File.directory?(local_dir)

    puts "Downloading #{limit || '∞'} files with date between #{start_date}..#{end_date} from bucket '#{bucket_name}' to '#{local_dir}''"

    s3 = Aws::S3::Resource.new
    bucket = s3.bucket(bucket_name)

    (start_date..end_date).each do |date|
      remote_tweets_folder = "tweets/#{date.strftime('%Y%m%d')}"
      remote_files = bucket.objects(prefix: remote_tweets_folder)
      remote_files.each do |remote_file|
        local_file_path = File.join([local_dir, "#{remote_file.key.split(File::SEPARATOR).last}"])
        File.open(local_file_path, 'wb') do |local_file|
          local_file.write(remote_file.get.body.read)
        end
        `lzop -d #{local_file_path}`
        File.delete(local_file_path)
        count += 1
        break unless limit.nil? || count < limit
      end
    end

    puts "Wrote #{count} files to #{local_dir}"
  end
  module_function :download_tweets

  def read_tweets_from_file(file_path)
    tweets = []
    File.open(file_path, 'r').read.each_line do |tweet|
      tweets.push(JSON.parse(tweet))
    end
    tweets
  end
  module_function :read_tweets_from_file

end
