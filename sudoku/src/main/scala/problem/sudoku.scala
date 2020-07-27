package problem
import scala.io.Source

object sudoku {
  
  def main(args:Array[String]):Unit = {
    loadPuzzle(args(0)) match {
      case Some(puzzle) => println(validate(puzzle))
      case None => sys.exit(1)
    }
  }
  
  def loadPuzzle(s:String):Option[List[List[Int]]]={
    try{
      val puzzle:List[String] = Source.fromFile(s).getLines.toList
      val fPuzzle = puzzle.map(row=>row.split(",").map(element=>element.toInt).toList)
      if(contentsCheck(fPuzzle)){
        Some(fPuzzle)
      }
      else{
        println("Invalid Sudoku puzzle. Valid puzzles must be 9x9 sized"); None
      }
    }
    catch{
      case _: java.io.FileNotFoundException => println("Could not find specified file"); None
      case _: java.lang.NumberFormatException => println("File could not be parsed"); None
      case _: java.io.IOException => println("IO Exception"); None
      case e: Exception => println(e); None
    }
  }
  
  def contentsCheck(fPuzzle:List[List[Int]]):Boolean = {
    val rightWidth = fPuzzle.map(row => {if (row.size == 9) true else false}).forall(_ != false)
    if(fPuzzle.length == 9 && rightWidth) true else false
  }
  
  def validate(puzzle:List[List[Int]]):String = {
    val puzzleT = puzzle.transpose
    val pVal1 = puzzle.map(validateLine).forall(_ == true)
    val pVal2 = puzzleT.map(validateLine).forall(_ == true)
    if(pVal1 && pVal2){
      "VALID"
    }
    else{
      "INVALID"
    }
  }
  
  def validateLine(line:List[Int]):Boolean = {
    if (line.sorted.foldLeft(1){ (a, x) => {if (x == a) a + 1 else a}} == 10) true else false
  }
}