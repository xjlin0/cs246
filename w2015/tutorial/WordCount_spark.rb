# in ruby-spark shell
# load('WordCount_spark.rb')
# require 'ruby-spark'
parseLine = lambda do |line|
  word_re = Regexp.new(/[\w']+/)
  line.scan(word_re).map{ |word| [word.downcase, 1] }
end

fileName = 'pg100.txt'

wordRDD = $sc
            .textFile( fileName )
            .flatMap( parseLine )
            .reduceByKey( lambda{ |a, b| a + b } )
            #using .cache may cause error

puts wordRDD.reduce( lambda{|memo, item| memo[1] > item[1] ? memo : item } )
#RDD.max does not take 'key' argument in ruby-spark!