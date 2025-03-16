package org.radix.hyperscale.tools.spam;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Documented
@Retention(RUNTIME)
@Target(TYPE)
public @interface SpamConfig
{
	String name();
	Option[] options() default {};
    
    @Retention(RUNTIME)
    @Target({})
    @interface Option 
    {
        String opt();
        String longOpt() default "";
        String description() default "";
        boolean hasArg() default false;
        boolean required() default false;
    }
}
