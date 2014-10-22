import java.nio.file.{StandardOpenOption, Path, Paths, Files}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import org.apache.commons.lang3.StringEscapeUtils
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/**
 * Created by Borislav Kapukaranov on 10/19/14.
 */
object Main {
  def main(args: Array[String]) {
    //args(0..1) - positive/negative data
    //args(2) - tweets file
    //args(3) - out SVM file

    // cleanup & prepare
    Files.deleteIfExists(Paths.get(args(3)))
    Files.createFile(Paths.get(args(3)))

    printSvmOutput(args, buildDictionary(args))
    println("done")
  }

  private def printSvmOutput(args: Array[String], dictionary: Map[String, Double]) {
    val words : List[String] = dictionary.keySet.toList
    println(dictionary.size)
    val tweetsFile = args(2)
    val outFile = args(3)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    Source
      .fromFile(tweetsFile)
      .getLines()
      .foreach { line =>
//      println(StringEscapeUtils.unescapeJava(line))
//      val pairVector: ListBuffer[(String, Double)] = ListBuffer()
      val doubleVector : Array[Double] = new Array[Double](words.size)
      // 0. separate label from tweet
      val tweetPair = line.split("\t")
      val label = tweetPair(2)
      val tweet = tweetPair(3)
      val unescapedTweet = StringEscapeUtils.unescapeJava(tweet)

      // 1. replace urls, users, email, remove hashtag symbols
      val normalizedTweet = unescapedTweet.replaceAll("https?://[^\\s]*", "URL")
        .replaceAll("(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9_]+)", "USERNAME")
        .replaceAll("[^\\s]+@[^\\s]+[.][^\\s]{2,4}", "EMAIL")
        .replaceAll("#", "")
      println(normalizedTweet)
      // 2. build vector
//      var positive = 0.0
//      var negative = 0.0

      var startTime = System.currentTimeMillis()
      for (index <- 0 until words.size) {
        if (normalizedTweet.contains(words(index))) {
//          println(words(index) + " " + dictionary(words(index)))
          doubleVector(index) = dictionary(words(index))
        } else {
          doubleVector(index) = 0.0
        }
      }
      var endTime = System.currentTimeMillis()
      print("building vector: ")
      println(endTime - startTime)
//      words.foreach { word =>
//        if (normalizedTweet.contains(word)) {
//          println(word + " " + dictionary(word))
//          if (dictionary(word) > 0) {
//            positive += dictionary(word)
//          }
//          else {
//            negative += dictionary(word)
//          }
//        }
//      }
//      words.foreach { word =>
//        // if word is present in tweet update vector
//        if (normalizedTweet.contains(word)) {
//          pairVector += Pair(word, dictionary(word))
//        } else {
//          pairVector += Pair(word, 0.0)
//        }
//      }

      // 3. assemble vector string
      val vectorString : StringBuilder = new StringBuilder(label)
//      for (index <- 0 to pairVector.length) {
//        vectorString.append(" %d %s".format(index + 1, pairVector(index)._2))
//      }
//      vectorString.append(" %d %s".format(1, positive))
//      vectorString.append(" %d %s".format(2, negative))
//      vectorString.append("\n")
      startTime = System.currentTimeMillis()
      for (index <- 0 until words.size) {
        vectorString.append( "%d %s".format(index+1, doubleVector(index)))
      }
      vectorString.append("\n")
      endTime = System.currentTimeMillis()
      print("building strint to write: ")
      println(endTime - startTime)

      // 4. print vector string
      val outPath : Path = Paths.get(outFile)
      Files.write(outPath, vectorString.toString().getBytes, StandardOpenOption.APPEND)
    }
  }

  private def buildDictionary(args: Array[String]): Map[String, Double] = {
    var dictionary: Map[String, Double] = Map()

    for (index <- 0 to 1) {
      Source
        .fromFile(args(index))
        .getLines()
        .foreach { line =>
        val lineTab = line.indexOf("\t")
        val lineWord = line.substring(0, lineTab)
        val lineValue = line.substring(lineTab).toDouble
        dictionary += (lineWord -> lineValue)
      }
    }
    dictionary
  }
}
