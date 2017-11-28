package datastructure

import java.time.LocalDateTime
/**
  * Created by marc on 10.05.17.
  */
class RowTransformationTime(lon : Int,
                            lat : Int,
                            var time : LocalDateTime)  extends RowTransformation(lon, lat){

}
