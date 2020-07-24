package test.scala
import org.scalatest.funsuite.AnyFunSuite
import problem.sudoku._
import java.io.ByteArrayOutputStream

class sudokuTests extends AnyFunSuite{
  test("correct puzzle should load into List[List[Int] with loadPuzzle"){
    val puzzle = loadPuzzle("./testfiles/valid.txt")
    assert(!puzzle.isEmpty)
    assert(puzzle.isInstanceOf[Option[List[List[Int]]]])
  }
  test("Incorrectly sized puzzle should be reported correctly by loadPuzzle"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(loadPuzzle("./testfiles/wrongsize.txt"))
    assert(out.toString().filter(_ >= ' ') == "Invalid Sudoku puzzle. Valid puzzles must be 9x9 sized")
  }
  test("Incorrectly formatted puzzle should be reported correctly by loadPuzzle"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(loadPuzzle("./testfiles/wrongformat.txt"))
    assert(out.toString().filter(_ >= ' ') == "File could not be parsed")
  }
  test("File not found should be reported correctly by loadPuzzle"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(loadPuzzle("./testfiles/null.txt"))
    assert(out.toString().filter(_ >= ' ') == "Could not find specified file")
  }  
  test("contentsCheck should return true for 9x9 nested list"){
    assert(contentsCheck(List.fill(9)(List.fill(9)(1))))
  }
  test("contentsCheck should return false for larger than 9x9 nested list"){
    assert(!contentsCheck(List.fill(10)(List.fill(10)(1))))
    assert(!contentsCheck(List.fill(9)(List.fill(10)(1))))
    assert(!contentsCheck(List.fill(10)(List.fill(9)(1))))
  }
  test("contentsCheck should return false for smaller than 9x9 nested list"){
    assert(!contentsCheck(List.fill(8)(List.fill(8)(1))))
    assert(!contentsCheck(List.fill(8)(List.fill(9)(1))))
    assert(!contentsCheck(List.fill(9)(List.fill(8)(1))))
  }  
  test("validateLine should return true for valid sudoku line"){
    assert(validateLine(List(1,2,3,4,5,6,7,8,9)))
    assert(validateLine(List(9,8,7,6,5,4,3,2,1)))
  }
  test("validateLine should return false for invalid sudoku line"){
    assert(!validateLine(List(2,3,4,5,6,7,8,9,10)))
    assert(!validateLine(List(9,8,7,9,5,4,3,2,1)))
  }  
  test("validate should return VALID for valid nested array"){
    val correct = List(
        List(1,2,3,4,5,6,7,8,9),
        List(2,3,4,5,6,7,8,9,1),
        List(3,4,5,6,7,8,9,1,2),
        List(4,5,6,7,8,9,1,2,3),
        List(5,6,7,8,9,1,2,3,4),
        List(6,7,8,9,1,2,3,4,5),
        List(7,8,9,1,2,3,4,5,6),
        List(8,9,1,2,3,4,5,6,7),
        List(9,1,2,3,4,5,6,7,8))
    assert(validate(correct) == "VALID")
  }
  test("main should print VALID for valid file"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(problem.sudoku.main(Array("./testfiles/valid.txt")))
    assert(out.toString().filter(_ >= ' ') == "VALID")    
  }  
  test("main should print INVALID for vertically invalid file"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(problem.sudoku.main(Array("./testfiles/invalidvertical.txt")))
    assert(out.toString().filter(_ >= ' ') == "INVALID")    
  }  
  test("main should print INVALID for horizontally invalid file"){
    val out = new ByteArrayOutputStream
    Console.withOut(out)(problem.sudoku.main(Array("./testfiles/invalidhorizontal.txt")))
    assert(out.toString().filter(_ >= ' ') == "INVALID")    
  }    
}
