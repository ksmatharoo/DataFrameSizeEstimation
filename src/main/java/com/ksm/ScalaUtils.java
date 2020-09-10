package com.ksm;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.List;

//this class as util functions which convert java types to corresponding scala types
//java to scala type
public class ScalaUtils {
    public static <T> ClassTag<T> getClassTag(Class<T> klass) {
        return scala.reflect.ClassManifestFactory.fromClass(klass);
    }

    public static <T> Seq<T> convertListToSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }
}
