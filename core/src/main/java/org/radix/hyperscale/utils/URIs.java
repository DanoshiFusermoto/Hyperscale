package org.radix.hyperscale.utils;

import java.net.URI;
import java.net.URISyntaxException;

public final class URIs {
  public static final URI toHostAndPort(URI uri) {
    try {
      if (uri.getScheme() != null
          || uri.getAuthority() != null
          || uri.getFragment() != null
          || uri.getPath() != null
          || uri.getQuery() != null
          || uri.getUserInfo() != null)
        return new URI(null, null, uri.getHost(), uri.getPort(), null, null, null);

      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private URIs() {
    // Dummy constructor
  }
}
