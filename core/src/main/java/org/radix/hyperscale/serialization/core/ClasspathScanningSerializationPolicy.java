package org.radix.hyperscale.serialization.core;

import java.util.Set;

import org.radix.hyperscale.serialization.ClassScanningSerializationPolicy;
import org.radix.hyperscale.serialization.SerializationPolicy;
import org.radix.hyperscale.serialization.SerializerConstants;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class that maintains a map of {@link DsonOutput.Output} types to
 * a set of pairs of classes and field/method names to output for that
 * serialization type.
 * <p>
 * This implementation works by scanning the classpath for classes
 * annotated with {@code SerializerConstants.SERIALIZER_ID_ANNOTATION} and
 * passing these classes to {@link ClassScanningSerializationPolicy}.
 */
public final class ClasspathScanningSerializationPolicy extends ClassScanningSerializationPolicy {

	/**
	 * Note that this class is expensive to create, potentially in the
	 * order of several seconds for a large classpath.
	 *
	 * @return A freshly created {@link ClasspathScanningSerializationPolicy}
	 */
	public static SerializationPolicy create() {
		return new ClasspathScanningSerializationPolicy();
	}


	@VisibleForTesting
	ClasspathScanningSerializationPolicy() {
		super(scanForSerializable());
	}

	private static Set<Class<?>> scanForSerializable() {
		Reflections reflections = getReflections("org.radix.hyperscale");
		return reflections.getTypesAnnotatedWith(SerializerConstants.SERIALIZER_ID_ANNOTATION);
    }

	private static Reflections getReflections(String... packageNames) {
		ConfigurationBuilder config = new ConfigurationBuilder()
			.setUrls(ClasspathHelper.forJavaClassPath())
			.filterInputsBy(new FilterBuilder().includePackage(packageNames));
		return new Reflections(config);
	}
}
