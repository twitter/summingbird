package com.twitter.summingbird.javaapi;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import scala.Option;
import scala.Some;
import scala.Tuple2;

import com.twitter.algebird.Semigroup;
import com.twitter.algebird.Semigroup$;
import com.twitter.summingbird.Platform;
import com.twitter.summingbird.javaapi.impl.JKeyedProducerImpl;
import com.twitter.summingbird.javaapi.impl.JProducerImpl;

public class JProducers {

  public static <P extends Platform<P>, T> JProducer<P, T> source(Source<P, ?, T> source) {
    return JProducerImpl.source(source);
  }

  public static <P extends Platform<P>, T, K, V> JKeyedProducer<P, K, V> toKeyed(JProducer<P, Tuple2<K, V>> producer) {
    return JKeyedProducerImpl.toKeyed(producer);
  }

  @SuppressWarnings("unchecked")
  public static <T> Option<T> none() {
    return (Option<T>)scala.None$.MODULE$;
  }

  public static <T> Option<T> some(T t) {
    return new Some<T>(t);
  }

  private static Map<Class<?>, Method> classToMethod;

  /**
   * building a registry of semigroup provider functions in object Semigroup
   */
  private static void initClassToMethod() {
    classToMethod = new HashMap<Class<?>, Method>();
    Class<? extends Semigroup$> sgClass = Semigroup$.MODULE$.getClass();
    for (Method method : sgClass.getMethods()) {
      if (method.getParameterTypes().length == 0 && method.getReturnType().isAssignableFrom(Semigroup.class)) {
        // method of signature {name}(): Semigroup[T]
        Type returnType = method.getGenericReturnType();
        if (returnType instanceof ParameterizedType) {
          Class<?> operandType;
          // get the Type argument from the return type Semigroup[T]
          Type type = ((ParameterizedType)returnType).getActualTypeArguments()[0];
          if (type instanceof ParameterizedType) {
            // if itself a parameterized type then just get the erased equivalent
            operandType = (Class<?>)((ParameterizedType)type).getRawType();
          } else {
            operandType = (Class<?>)type;
          }
          // because of type erasure scala primitive types just become Object
          if (operandType != Object.class) {
            if (classToMethod.put(operandType, method) != null) {
              classToMethod.remove(operandType);
              // let's not create ambiguity
            }
          }
        }
      }
    }
  }

  private static Method getMethod(Class<?> operandType) {
    if (classToMethod == null) initClassToMethod();
    Method method = classToMethod.get(operandType);
    if (method == null && operandType.getSuperclass() != null) {
      return getMethod(operandType.getSuperclass());
    } else {
      return method;
    }
  }

  public static <T> Semigroup<T> semigroup(Class<T> c) {
    Method method = getMethod(c);
    if (method != null) {
      try {
        @SuppressWarnings("unchecked")
        Semigroup<T> sg = (Semigroup<T>)method.invoke(Semigroup$.MODULE$);
        return sg;
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Should not happen. Could not resolve semigroup for " + c,  e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Should not happen. Could not resolve semigroup for " + c,  e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Could not resolve semigroup for " + c + " Semigroup." + method.getName() + "() threw an exception",  e.getTargetException());
      }
    } else {
      throw new IllegalArgumentException("No known semigroup for class " + c);
    }
  }
}
