#code executed in Spark Core Command line
#top 10 calling destinations

val cdrFile=sc.textFile("hdfs://zwnn1.localdomain:9000/cdr/cdr.txt")
val dest=cdrFile.map(_.split('')).filter(x=>x(2).toInt==1).map(x=>x(14))
dest.countByValue().toSeq.sortBy(_._2).reverse.take(10).foreach(println)

#top 10 caller

val cdrFile=sc.textFile("hdfs://zwnn1.localdomain:9000/cdr/cdr.txt")
val caller=cdrFile.map(_.split('')).filter(x=>x(2).toInt==1).mao(x=> {x(13)->(x(9).toInt,x(10).toInt,1)})
caller.reduceByKey((a,b)=> {a._1+b._1,a._2+b._2,a._3+b._3)}).collect()sortBy(_.2._2).reverse.take(10).foreach(println)

#Revenue based on Service (Voice Call/SMS/Data)

def getCDRTypeName(cdrType:Int):String ={
if(cdrType==1)
"VoiceCall"
else if((cdrType==206)||(cdrType==207))
"SMS"
elseif((cdrType==58)||(cdrType==59))
"DATA"
else
"Other"
}
val cdrFile=sc.textFile("hdfs://zwnn1.localdomain:9000/cdr/cdr.txt")
val services=cdrFile.map(_.split('')).filter(x=>2(x(2).toInt==1||x(2).toInt==206||x(2).toInt==207||x(2).toInt==59||x(2).toInt==58)).map(x=>{getCDRTypeName(x(2).toInt)->x(10).toInt))
services.reduceByKey(_+_).collect().foreach(println)