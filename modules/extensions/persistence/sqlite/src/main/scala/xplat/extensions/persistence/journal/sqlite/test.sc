import geny.Generator

val gen = Generator.from(Seq(1, 2, 3, 4))



gen.generate{ x=>
  println(x)
  Generator.End
}

gen.generate{ x=>
  println(x)
  Generator.Continue
}