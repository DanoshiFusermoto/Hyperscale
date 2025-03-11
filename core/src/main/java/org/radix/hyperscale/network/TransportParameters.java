package org.radix.hyperscale.network;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Documented
@Inherited
@Retention(RUNTIME)
@Target(TYPE)
public @interface TransportParameters 
{
	int priority() default 0;
	int weight() default 1;
	boolean urgent() default false;
	boolean async() default false;
	boolean cache() default false;
}
