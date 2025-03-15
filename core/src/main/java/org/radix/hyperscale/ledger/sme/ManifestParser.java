package org.radix.hyperscale.ledger.sme;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.VotePowers;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.arguments.AccountArgument;
import org.radix.hyperscale.ledger.sme.arguments.BadgeArgument;
import org.radix.hyperscale.ledger.sme.arguments.HashArgument;
import org.radix.hyperscale.ledger.sme.arguments.IdentityArgument;
import org.radix.hyperscale.ledger.sme.arguments.TokenArgument;
import org.radix.hyperscale.ledger.sme.arguments.UInt256Argument;
import org.radix.hyperscale.ledger.sme.arguments.VaultArgument;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.utils.UInt256;

public class ManifestParser {
  private static final int MANIFEST_MAX_INSTRUCTION_ARGUMENTS = 16;
  private static final int MANIFEST_ARGUMENT_CACHE_SIZE = 2 << 16;
  private static final int MANIFEST_ARGUMENT_MAX_LENGTH = 64;
  private static final LRUCacheMap<String, Argument<?>> argCache =
      new LRUCacheMap<String, Argument<?>>(MANIFEST_ARGUMENT_CACHE_SIZE);

  public static List<Object> parse(final List<String> manifest) throws ManifestException {
    ManifestParser parser = new ManifestParser(manifest.size());
    return Collections.unmodifiableList(parser.convert(manifest));
  }

  private final List<Object> objects;

  private ManifestParser(final int size) {
    this.objects = new ArrayList<Object>(size);
  }

  private List<Object> convert(final List<String> manifest) throws ManifestException {
    for (int i = 0; i < manifest.size(); i++) {
      final String entry = manifest.get(i);

      // blob
      if (entry.startsWith("data:")) {
        Blob blob = blob(entry);
        this.objects.add(new Persist(blob));
        continue;
      }

      // instructions
      int startBracketIndex = entry.indexOf('(');
      if (startBracketIndex > -1) {
        // native instruction
        int namespaceIndex = indexOf(entry, "::", 0, startBracketIndex);
        if (namespaceIndex > 0) {
          if (startBracketIndex < namespaceIndex + 2)
            throw new ManifestException("Native method call syntax error: " + entry);

          if (startBracketIndex >= entry.indexOf(')'))
            throw new ManifestException("Native method call syntax error: " + entry);

          if (entry.startsWith("blueprint::deploy")) {
            Deploy deploy = deploy(entry);
            this.objects.add(deploy);
          } else if (entry.startsWith("blueprint::instantiate")) {
            Instantiate instantiate = instantiate(entry);
            this.objects.add(instantiate);
          } else if (entry.startsWith("ledger::epoch")) {
            Epoch epoch = epoch(entry);
            this.objects.add(epoch);
          } else if (entry.startsWith("ledger::shards")) {
            Shards shards = shards(entry);
            this.objects.add(shards);
          } else {
            Method method = method(entry);
            this.objects.add(method);
          }

          continue;
        }

        // component method call
        namespaceIndex = entry.lastIndexOf('.', startBracketIndex);
        if (namespaceIndex > 0) {
          if (entry.indexOf('(') < namespaceIndex + 1)
            throw new ManifestException("Component method call syntax error: " + entry);

          if (entry.indexOf('(') >= entry.indexOf(')'))
            throw new ManifestException("Component method call syntax error: " + entry);

          Method method = method(entry);
          this.objects.add(method);
          continue;
        }
      }

      // argument objects
      final String subArgument = entry.substring(0, startBracketIndex);
      if (subArgument.equalsIgnoreCase("badge")) {
        BadgeArgument badge = badge(entry);
        this.objects.add(badge);
        continue;
      }

      if (subArgument.equalsIgnoreCase("account")) {
        AccountArgument account = account(entry);
        this.objects.add(account);
        continue;
      }

      if (subArgument.equalsIgnoreCase("token")) {
        TokenArgument token = token(entry);
        this.objects.add(token);
        continue;
      }

      throw new UnsupportedOperationException("Unknown manifest type: " + entry);
    }

    return this.objects;
  }

  private Blob getBlob(final HashArgument hash) {
    for (final Object object : this.objects) {
      if (object instanceof Persist persist) {
        if (persist.getBlob().getHash().equals(hash.get())) return persist.getBlob();
      }
    }

    return null;
  }

  private Deploy deploy(final String entry) throws ManifestException {
    final Object[] arguments = parseArguments(entry);

    final String name = (String) arguments[0];
    final String language = (String) arguments[1];
    final Identity identity = ((IdentityArgument) arguments[3]).get();

    // Discover the blob package, must be embedded within a Persist instruction!
    Blob codeBlob = getBlob((HashArgument) arguments[2]);
    if (codeBlob == null)
      throw new ManifestException(
          "Code blob " + arguments[2] + " not found for blueprint::deploy " + arguments);

    return new Deploy(name, language, codeBlob, identity);
  }

  private Epoch epoch(final String entry) throws ManifestException {
    final Object[] arguments = parseArguments(entry);

    final long clock = (long) arguments[0];
    final ShardGroupID shardGroupID = ShardGroupID.from((long) arguments[1]);
    final Blob powersBlob = getBlob(((HashArgument) arguments[2]));

    if (powersBlob == null)
      throw new ManifestException(
          "Powers blob " + arguments[2] + " not found for native::epoch " + arguments);

    final VotePowers powers;
    try {
      powers =
          Serialization.getInstance()
              .fromJson(
                  new String(powersBlob.getBytes(), StandardCharsets.UTF_8), VotePowers.class);
    } catch (SerializationException sex) {
      throw new ManifestException(
          "Failed to deserialize vote powers for native::epoch " + arguments, sex);
    }

    return new Epoch(clock, shardGroupID, powers);
  }

  private Shards shards(final String entry) throws ManifestException {
    final Object[] arguments = parseArguments(entry);

    final long epoch = (long) arguments[0];
    final int current = (int) arguments[1];
    final int proposed = (int) arguments[2];

    return new Shards(epoch, current, proposed);
  }

  private Instantiate instantiate(final String entry) throws ManifestException {
    final Object[] arguments = parseArguments(entry);

    final String blueprint = (String) arguments[0];
    final String context = (String) arguments[1];
    final Identity identity = ((IdentityArgument) arguments[2]).get();
    final Object[] constructorArguments = (Object[]) arguments[3];

    return new Instantiate(blueprint, context, identity, constructorArguments);
  }

  private Method method(final String entry) throws ManifestException {
    final String component;
    final String name;
    final Method method;
    final Object[] arguments = parseArguments(entry);

    // Native
    int namespaceIndex = entry.indexOf("::");
    if (namespaceIndex > 0) {
      component = entry.substring(0, namespaceIndex);
      name = entry.substring(namespaceIndex + 2, entry.indexOf('('));

      final StateAddress componentAddress = StateAddress.from(Component.class, component);
      final Class<? extends NativeComponent> nativeComponent =
          Component.getNative(componentAddress);
      if (nativeComponent == null)
        throw new ManifestException("Referenced native component not found: " + entry);

      method = new Method(componentAddress, component, name, arguments, true);
    } else {
      namespaceIndex = entry.lastIndexOf('.');
      component = entry.substring(0, namespaceIndex);
      name = entry.substring(namespaceIndex + 1, entry.indexOf('('));

      final StateAddress componentAddress = StateAddress.from(Component.class, component);
      final Class<? extends NativeComponent> nativeComponent =
          Component.getNative(componentAddress);
      if (nativeComponent != null)
        throw new ManifestException("Referenced component is native: " + entry);

      method = new Method(componentAddress, component, name, arguments, false);
    }

    return method;
  }

  private BadgeArgument badge(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof BadgeArgument badgeArgument) return badgeArgument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length != 1)
      throw new ManifestException("Expected single argument for badge: " + entry);

    final Identity identity;
    try {
      identity = Identity.from((String) arguments[0]);
    } catch (Exception cex) {
      throw new ManifestException("Failed to parse badge argument: " + entry, cex);
    }

    final BadgeArgument badgeArgument = new BadgeArgument(identity);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) argCache.putIfAbsent(entry, badgeArgument);

    return badgeArgument;
  }

  private Blob blob(final String entry) throws ManifestException {
    try {
      return new Blob(entry);
    } catch (Exception ex) {
      throw new ManifestException("Failed to decode Blob", ex);
    }
  }

  private IdentityArgument identity(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof IdentityArgument identityArgument) return identityArgument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length != 1)
      throw new ManifestException("Expected single argument for identity: " + entry);

    final Identity identity;
    try {
      identity = Identity.from((String) arguments[0]);
    } catch (Exception cex) {
      throw new ManifestException("Failed to parse badge identity: " + entry, cex);
    }

    final IdentityArgument identityArgument = new IdentityArgument(identity);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH)
      argCache.putIfAbsent(entry, identityArgument);

    return identityArgument;
  }

  private HashArgument hash(final String entry) throws ManifestException {
    final Object[] arguments = parseArguments(entry);
    if (arguments.length != 1)
      throw new ManifestException("Expected single argument for hash: " + entry);

    final Hash hash = Hash.from((String) arguments[0]);
    return new HashArgument(hash);
  }

  private UInt256Argument uint256(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof UInt256Argument uint256Argument) return uint256Argument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length != 1)
      throw new ManifestException("Expected single argument for uint256: " + entry);

    final UInt256 uint256;
    if (arguments[0] instanceof Long value) uint256 = UInt256.from(value);
    else if (arguments[0] instanceof String string) uint256 = UInt256.from(string);
    else if (arguments[0] instanceof UInt256 value) uint256 = value;
    else throw new ManifestException("Unsupported argument type for unint256: " + arguments[0]);

    final UInt256Argument uint256Argument = new UInt256Argument(uint256);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH)
      argCache.putIfAbsent(entry, uint256Argument);

    return uint256Argument;
  }

  private AccountArgument account(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof AccountArgument accountArgument) return accountArgument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length == 0 || arguments.length > 2)
      throw new ManifestException("Unexpected argument count for account: " + entry);

    final Identity identity;
    try {
      identity = Identity.from((String) arguments[0]);
    } catch (Exception cex) {
      throw new ManifestException("Failed to parse account identity: " + entry, cex);
    }

    StateLockMode lock = StateLockMode.WRITE;
    if (arguments.length > 1) lock = (StateLockMode) arguments[1];

    final AccountArgument accountArgument = new AccountArgument(identity, lock);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH)
      argCache.putIfAbsent(entry, accountArgument);

    return accountArgument;
  }

  private TokenArgument token(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof TokenArgument tokenArgument) return tokenArgument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length == 0 || arguments.length > 2)
      throw new ManifestException("Unexpected argument count for token: " + entry);

    final String symbol = (String) arguments[0];
    StateLockMode lock = StateLockMode.READ;
    if (arguments.length > 1) lock = (StateLockMode) arguments[1];

    final TokenArgument tokenArgument = new TokenArgument(symbol, lock);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) argCache.putIfAbsent(entry, tokenArgument);

    return tokenArgument;
  }

  private VaultArgument vault(final String entry) throws ManifestException {
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) {
      final Argument<?> cachedArgument = argCache.get(entry);
      if (cachedArgument instanceof VaultArgument vaultArgument) return vaultArgument;
    }

    final Object[] arguments = parseArguments(entry);
    if (arguments.length == 0 || arguments.length > 2)
      throw new ManifestException("Unexpected argument count for vault: " + entry);

    final Identity identity;
    try {
      identity = Identity.from((String) arguments[0]);
    } catch (Exception cex) {
      throw new ManifestException("Failed to parse vault identity: " + entry, cex);
    }

    StateLockMode lock = StateLockMode.WRITE;
    if (arguments.length > 1) lock = (StateLockMode) arguments[1];

    final VaultArgument vaultArgument = new VaultArgument(identity, lock);
    if (entry.length() <= MANIFEST_ARGUMENT_MAX_LENGTH) argCache.putIfAbsent(entry, vaultArgument);

    return vaultArgument;
  }

  private final int TYPE_NONE = 0;
  private final int TYPE_SQR_BRACKETS = 1;
  private final int TYPE_RND_BRACKETS = 2;

  private Object[] parseArguments(final String entry) throws ManifestException {
    int enclosingType = TYPE_NONE;
    int argumentsStart = -1;
    int argumentsEnd = -1;
    for (int c = 0; c < entry.length(); c++) {
      char current = entry.charAt(c);
      if (current == '(') {
        enclosingType = TYPE_RND_BRACKETS;
        argumentsStart = c;
        break;
      }

      if (current == '[') {
        enclosingType = TYPE_SQR_BRACKETS;
        argumentsStart = c;
        break;
      }
    }

    if (enclosingType == TYPE_NONE)
      throw new ManifestException("Failed to find arguments enclosing type");

    argumentsEnd = entry.lastIndexOf(enclosingType == TYPE_RND_BRACKETS ? ')' : ']');

    int numArguments = 0;
    final String[] splits = splitArguments(entry, argumentsStart + 1, argumentsEnd);
    for (; numArguments < splits.length; numArguments++) {
      if (splits[numArguments] == null) break;
    }

    final Object[] arguments = new Object[numArguments];
    for (int i = 0; i < numArguments; i++) {
      final String argument = splits[i];
      if (argument == null) break;

      // string
      if (argument.charAt(0) == '\"' || argument.charAt(0) == '\'') {
        arguments[i] = argument.substring(1, argument.length() - 1);
        continue;
      }

      // number
      if ((argument.charAt(0) >= '0' && argument.charAt(0) <= '9')
          || (argument.charAt(0) == '-'
              && argument.charAt(1) >= '0'
              && argument.charAt(1) <= '9')) {
        try {
          Number number = Long.parseLong(argument);
          arguments[i] = number;
          continue;
        } catch (NumberFormatException nfe) {
          /* Not a long:  Ignored to continue to next type test */
        }

        try {
          UInt256 number = UInt256.from(argument);
          arguments[i] = number;
          continue;
        } catch (NumberFormatException nfe) {
          /* Not a uint256:  Ignored to continue to next type test */
        }

        try {
          Number number = Double.parseDouble(argument);
          arguments[i] = number;
          continue;
        } catch (NumberFormatException nfe) {
        }
      }

      // objects
      int subType = TYPE_NONE;
      for (int c = 0; c < argument.length(); c++) {
        char current = argument.charAt(c);
        if (current == '(') {
          subType = TYPE_RND_BRACKETS;
          break;
        }

        if (current == '[') {
          subType = TYPE_SQR_BRACKETS;
          break;
        }
      }

      // argument objects
      if (subType == TYPE_RND_BRACKETS) {
        if (argument.startsWith("vault")) {
          VaultArgument vault = vault(argument);
          arguments[i] = vault;
          continue;
        }

        if (argument.startsWith("token")) {
          TokenArgument token = token(argument);
          arguments[i] = token;
          continue;
        }

        if (argument.startsWith("uint256")) {
          UInt256Argument uint256 = uint256(argument);
          arguments[i] = uint256;
          continue;
        }

        if (argument.startsWith("hash")) {
          HashArgument hash = hash(argument);
          arguments[i] = hash;
          continue;
        }

        if (argument.startsWith("identity")) {
          IdentityArgument identity = identity(argument);
          arguments[i] = identity;
          continue;
        }

        if (argument.startsWith("account")) {
          AccountArgument account = account(argument);
          arguments[i] = account;
          continue;
        }

        if (argument.startsWith("badge")) {
          BadgeArgument badge = badge(argument);
          arguments[i] = badge;
          continue;
        }
      }

      // extended arguments
      if (subType == TYPE_SQR_BRACKETS) {
        arguments[i] = parseArguments(argument);
        continue;
      }

      // lock
      try {
        if (argument.equalsIgnoreCase("READ")) {
          arguments[i] = StateLockMode.READ;
          continue;
        } else if (argument.equalsIgnoreCase("WRITE")) {
          arguments[i] = StateLockMode.WRITE;
          continue;
        }
      } catch (IllegalArgumentException iaex) {
        /* Not a state lock mode:  Ignored to continue to next type test */
      }

      // boolean
      if (argument.equalsIgnoreCase("true") || argument.equalsIgnoreCase("false")) {
        arguments[i] = Boolean.parseBoolean(argument);
        continue;
      }

      throw new IllegalArgumentException("Unsupported argument type: " + argument);
    }

    return arguments;
  }

  private String[] splitArguments(final String input, final int start, final int end)
      throws ManifestException {
    final String[] arguments = new String[MANIFEST_MAX_INSTRUCTION_ARGUMENTS];
    final char[] argumentChars = new char[end - start];
    int argumentLength = 0;
    int argumentIndex = 0;

    boolean inQuotes = false;
    int bracketDepth = 0;
    int parenthesisDepth = 0;

    for (int i = start; i < end; i++) {
      final char c = input.charAt(i);

      switch (c) {
        case '(':
          if (inQuotes == false) parenthesisDepth++;

          argumentChars[argumentLength++] = c;
          break;
        case ')':
          if (inQuotes == false && parenthesisDepth > 0) parenthesisDepth--;

          argumentChars[argumentLength++] = c;
          break;
        case ',':
          // Check if the comma is a delimiter
          if (inQuotes == false && bracketDepth == 0 && parenthesisDepth == 0) {
            // Found a valid delimiter, add the current argument to the list
            arguments[argumentIndex] = trim(argumentChars, 0, argumentLength);
            argumentLength = 0; // Reset the current argument
            argumentIndex++;

            if (argumentIndex == ManifestParser.MANIFEST_MAX_INSTRUCTION_ARGUMENTS)
              throw new ManifestException("Too many arguments when parsing input: " + input);
          } else {
            // Otherwise, treat the comma as part of the argument
            argumentChars[argumentLength++] = c;
          }
          break;
        case '"':
          inQuotes = !inQuotes; // Toggle the inQuotes flag
          argumentChars[argumentLength++] = c; // Add the quote to the current argument
          break;
        case '[':
          if (inQuotes == false) bracketDepth++;

          argumentChars[argumentLength++] = c;
          break;
        case ']':
          if (inQuotes == false && bracketDepth > 0) bracketDepth--;

          argumentChars[argumentLength++] = c;
          break;
        default:
          // Regular character, just add to the current argument
          argumentChars[argumentLength++] = c;
          break;
      }
    }

    // Add the last argument if there's any content left
    if (argumentLength > 0) arguments[argumentIndex] = trim(argumentChars, 0, argumentLength);

    return arguments;
  }

  private String trim(char[] array, int offset, int length) {
    if (array == null || length <= 0 || offset < 0 || offset + length > array.length) return "";

    int start = offset;
    int end = offset + length - 1;

    while (start <= end && Character.isWhitespace(array[start])) {
      start++;
    }

    if (start > end) return "";

    while (end > start && Character.isWhitespace(array[end])) {
      end--;
    }

    return new String(array, start, end - start + 1);
  }

  private int indexOf(String string, String query, int beginIndex, int endIndex) {
    if (query.isEmpty()) return beginIndex;

    outer:
    for (int i = beginIndex; i <= endIndex - query.length(); i++) {
      for (int j = 0; j < query.length(); j++) {
        if (string.charAt(i + j) != query.charAt(j)) {
          continue outer;
        }
      }
      return i;
    }

    return -1;
  }
}
