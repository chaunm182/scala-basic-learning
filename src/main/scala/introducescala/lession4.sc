

//tuple - immutable
val tuple = ("a","b","c");
println(tuple)
println(tuple._1)
println(tuple._2)

val a = "b" -> "c";
println(a)

//List
val shipList = List("a","b","c", 1);
print(shipList)
print(shipList(1))
print(shipList.tail)

for(ship <- shipList){
  println(ship)
}

val numberList = List(1,2,3,4,5);
val sum = numberList.reduce((x,y) => x+y)
print(sum)

//filter() removes stuff
val l = numberList.filter(x => x!=5);
