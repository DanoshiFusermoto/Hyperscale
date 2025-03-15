package org.radix.hyperscale;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public interface Plugin {
  /**
   * Loads all components of a type hierarchy, including interfaces, abstract classes, and concrete
   * implementations to ensure all static blocks are executed.
   *
   * @param packageName The base package to scan
   * @param baseType The root type (interface or class) for the hierarchy
   * @param callback A function to call on successful class loading
   */
  static <T> void loadTypeHierarchy(
      final String packageName, final Class<T> baseType, final Consumer<Class<?>> callback) {
    final Logger log = Logging.getLogger();
    final Reflections reflections =
        new Reflections(
            new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(packageName))
                .setScanners(new SubTypesScanner(false)));

    // Get all types that extend/implement the base type
    final Set<Class<? extends T>> allSubTypes = reflections.getSubTypesOf(baseType);
    final Set<Class<?>> typesToLoad = new LinkedHashSet<Class<?>>();

    // Add the base type first (if it's not Object)
    if (baseType.equals(Object.class) == false) typesToLoad.add(baseType);

    final Set<Class<?>> interfaces =
        allSubTypes.stream().filter(Class::isInterface).collect(Collectors.toSet());
    typesToLoad.addAll(interfaces);

    final Set<Class<?>> abstractClasses =
        allSubTypes.stream()
            .filter(clazz -> !clazz.isInterface() && Modifier.isAbstract(clazz.getModifiers()))
            .collect(Collectors.toSet());
    typesToLoad.addAll(abstractClasses);

    final Set<Class<?>> concreteClasses =
        allSubTypes.stream()
            .filter(
                clazz -> !clazz.isInterface() && Modifier.isAbstract(clazz.getModifiers()) == false)
            .collect(Collectors.toSet());
    typesToLoad.addAll(concreteClasses);

    if (log.hasLevel(Logging.DEBUG)) {
      log.debug("Found types in hierarchy:");
      log.debug(
          "- Base type: "
              + (baseType.isInterface() ? "interface" : "class")
              + " "
              + baseType.getName());
      log.debug("- Interfaces: " + interfaces.size());
      log.debug("- Abstract classes: " + abstractClasses.size());
      log.debug("- Concrete classes: " + concreteClasses.size());
    }

    final Set<Class<?>> loadedTypes = new HashSet<Class<?>>();
    for (final Class<?> type : typesToLoad) {
      if (loadedTypes.contains(type) == false) {
        // Load all parents first
        loadParentTypesRecursively(type, loadedTypes);

        // Now load this type
        try {
          // For interfaces, just add to loaded set (no static blocks)
          if (type.isInterface()) {
            loadedTypes.add(type);
            if (log.hasLevel(Logging.DEBUG)) log.debug("Processed interface: " + type.getName());
          } else {
            // For classes, force loading to execute static blocks
            Class.forName(type.getName());
            loadedTypes.add(type);

            if (log.hasLevel(Logging.DEBUG))
              log.debug(
                  "Loaded "
                      + (Modifier.isAbstract(type.getModifiers())
                          ? "abstract class: "
                          : "concrete class: ")
                      + type.getName());
          }

          if (callback != null) callback.accept(type);
        } catch (ClassNotFoundException | ExceptionInInitializerError ex) {
          System.err.println("Error loading type: " + type.getName());
          log.error("Error loading type: " + type.getName(), ex);
        }
      }
    }
  }

  /** Recursively loads parent types before loading a child type */
  private static void loadParentTypesRecursively(
      final Class<?> type, final Set<Class<?>> loadedTypes) {
    final Logger log = Logging.getLogger();

    // Skip if already loaded or Object
    if (loadedTypes.contains(type) || type.equals(Object.class)) return;

    // First load superclass (if not Object)
    final Class<?> superclass = type.getSuperclass();
    if (superclass != null && superclass.equals(Object.class) == false) {
      loadParentTypesRecursively(superclass, loadedTypes);

      // Force load the superclass if it's not an interface
      if (superclass.isInterface() == false && loadedTypes.contains(superclass) == false) {
        try {
          Class.forName(superclass.getName());
          loadedTypes.add(superclass);
          if (log.hasLevel(Logging.DEBUG))
            log.debug(
                "Loaded parent "
                    + (Modifier.isAbstract(superclass.getModifiers())
                        ? "abstract class: "
                        : "class: ")
                    + superclass.getName());
        } catch (ClassNotFoundException | ExceptionInInitializerError ex) {
          System.err.println("Error loading parent class: " + superclass.getName());
          log.error("Error loading parent class: " + type.getName(), ex);
        }
      }
    }

    // Then load all interfaces
    for (final Class<?> iface : type.getInterfaces()) {
      loadParentTypesRecursively(iface, loadedTypes);

      if (loadedTypes.contains(iface) == false) {
        loadedTypes.add(iface);
        if (log.hasLevel(Logging.DEBUG))
          log.debug("Processed parent interface: " + iface.getName());
      }
    }
  }
}
