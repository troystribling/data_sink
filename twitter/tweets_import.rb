require './tweets'

default_date = Date.today.strftime('%Y%m%d')
start_date = Date.parse(ARGV[0] || default_date)
end_date = Date.parse(ARGV[1] || default_date)
