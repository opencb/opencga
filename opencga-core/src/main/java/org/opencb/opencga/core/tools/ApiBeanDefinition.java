package org.opencb.opencga.core.tools;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ApiBeanDefinition {
    String value() default "";

}