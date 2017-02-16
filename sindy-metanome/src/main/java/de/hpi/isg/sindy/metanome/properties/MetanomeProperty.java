package de.hpi.isg.sindy.metanome.properties;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Exposes algorithm properties to Metanome via reflection.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MetanomeProperty {

    String name() default "";

    boolean required() default false;

}
