package org.poem.sort

/**
  * 排序
  * @param first
  * @param second
  */
class Second_Sort_Key(val first: Double, val second: Double) extends Ordered[Second_Sort_Key] with Serializable {


  /** Result of comparing `this` with operand `that`.
    *
    * Implement this method to determine how instances of A will be sorted.
    *
    * Returns `x` where:
    *
    *   - `x < 0` when `this < that`
    *
    *   - `x == 0` when `this == that`
    *
    *   - `x > 0` when  `this > that`
    *
    */
  override def compare(that: Second_Sort_Key): Int = {
    // 首先判断第一个排序字段是否相等，如果不想等， 直接排序
    if (this.first - that.first != 0) {
      (this.first - that.first).toInt
    } else {
      //如果第一个字段相等， 则比较第二个字段，如果像实现多次排序， 也可以按照这个模式继续比较下去
      if (this.second - that.second > 0) {
        Math.ceil(this.second - that.second).toInt
      } else if (this.second - that.second < 0) {
        Math.floor(this.second - that.second).toInt
      } else {
        (this.second - that.second).toInt
      }
    }
  }
}
