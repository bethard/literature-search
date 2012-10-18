package info.bethard.litsearch

import org.apache.lucene.queries.function.ValueSource
import org.apache.lucene.queries.function.valuesource.SimpleFloatFunction
import org.apache.lucene.queries.function.FunctionValues
import org.apache.lucene.queries.function.valuesource.ScaleFloatFunction

object QueryFunctions {

  val logOf1Plus = (source: ValueSource) => new SimpleFloatFunction(source) {
    override def name: String = "logOf1Plus"
    override def func(doc: Int, vals: FunctionValues): Float = {
      math.log(vals.doubleVal(doc) + 1.0).toFloat
    }
  }

  val scaleBetween = (min: Float, max: Float) => (source: ValueSource) => {
    new ScaleFloatFunction(source, min, max)
  }
}

