# load('WordCount_spark.rb') in ruby-spark shell, no need to Spark.start
# or (jenv exec) ruby WordCount_spark.rb in terminal shell
require 'ruby-spark'
Spark.start
parseLine = lambda do |line|
  word_re = Regexp.new(/[\w']+/)
  line.scan(word_re).map{ |word| [word.downcase, 1] }
end

fileName = 'pg100.txt'

wordRDD = Spark.sc
            .textFile( fileName )
            .flatMap( parseLine )
            .reduceByKey( lambda{ |a, b| a + b } )
            #using .cache may cause error

puts wordRDD.reduce( lambda{|memo, item| memo[1] > item[1] ? memo : item } )
#RDD.max does not take 'key' argument in ruby-spark!
Spark.stop