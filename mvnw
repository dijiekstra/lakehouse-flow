#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
WRAPPER_DIR="$ROOT_DIR/.mvn/wrapper"
WRAPPER_PROPERTIES="$WRAPPER_DIR/maven-wrapper.properties"

if [ -f "$ROOT_DIR/.mavenrc" ]; then
  # shellcheck disable=SC1091
  . "$ROOT_DIR/.mavenrc"
fi

if [ -z "${JAVA_HOME:-}" ]; then
  echo "JAVA_HOME is not set. Lakehouse Flow requires JDK 17." >&2
  exit 1
fi

JAVA_BIN="$JAVA_HOME/bin/java"
if [ ! -x "$JAVA_BIN" ]; then
  echo "JAVA_HOME does not point to a usable JDK: $JAVA_HOME" >&2
  exit 1
fi

JAVA_MAJOR="$("$JAVA_BIN" -version 2>&1 | awk -F '[\".]' '/version/ { print $2; exit }')"
if [ "$JAVA_MAJOR" != "17" ]; then
  echo "Lakehouse Flow requires JDK 17, but JAVA_HOME reports Java $JAVA_MAJOR: $JAVA_HOME" >&2
  exit 1
fi

if [ ! -f "$WRAPPER_PROPERTIES" ]; then
  echo "Missing Maven wrapper properties: $WRAPPER_PROPERTIES" >&2
  exit 1
fi

distribution_url="$(grep '^distributionUrl=' "$WRAPPER_PROPERTIES" | cut -d= -f2- | sed 's#\\:#:#g')"
archive_name="$(basename "$distribution_url")"
distribution_name="${archive_name%-bin.tar.gz}"
distribution_dir="$WRAPPER_DIR/dists"
maven_home="$distribution_dir/$distribution_name"

download() {
  url="$1"
  output="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$output"
  elif command -v wget >/dev/null 2>&1; then
    wget -q "$url" -O "$output"
  else
    echo "Neither curl nor wget is available to download Maven." >&2
    exit 1
  fi
}

append_proxy_opts() {
  proxy_url="$1"
  protocol="$2"

  if [ -z "$proxy_url" ]; then
    return
  fi

  proxy_endpoint="${proxy_url#*://}"
  proxy_endpoint="${proxy_endpoint#*@}"
  proxy_endpoint="${proxy_endpoint%%/*}"
  proxy_host="${proxy_endpoint%:*}"
  proxy_port="${proxy_endpoint##*:}"

  if [ -z "$proxy_host" ] || [ -z "$proxy_port" ] || [ "$proxy_host" = "$proxy_port" ]; then
    return
  fi

  MAVEN_OPTS="${MAVEN_OPTS:-} -D$protocol.proxyHost=$proxy_host -D$protocol.proxyPort=$proxy_port"
  export MAVEN_OPTS
}

HTTP_PROXY_VALUE="${http_proxy:-}"
if [ -z "$HTTP_PROXY_VALUE" ]; then
  HTTP_PROXY_VALUE="${HTTP_PROXY:-}"
fi
HTTPS_PROXY_VALUE="${https_proxy:-}"
if [ -z "$HTTPS_PROXY_VALUE" ]; then
  HTTPS_PROXY_VALUE="${HTTPS_PROXY:-}"
fi
NO_PROXY_VALUE="${no_proxy:-}"
if [ -z "$NO_PROXY_VALUE" ]; then
  NO_PROXY_VALUE="${NO_PROXY:-}"
fi

append_proxy_opts "$HTTP_PROXY_VALUE" http
append_proxy_opts "$HTTPS_PROXY_VALUE" https
if [ -n "$NO_PROXY_VALUE" ]; then
  MAVEN_OPTS="${MAVEN_OPTS:-} -Dhttp.nonProxyHosts=$(printf '%s' "$NO_PROXY_VALUE" | tr ',' '|')"
  export MAVEN_OPTS
fi

GENERATED_SETTINGS=""
if [ -n "$HTTP_PROXY_VALUE" ] || [ -n "$HTTPS_PROXY_VALUE" ]; then
  GENERATED_SETTINGS="$WRAPPER_DIR/generated-settings.xml"
  non_proxy_hosts="$(printf '%s' "$NO_PROXY_VALUE" | tr ',' '|')"

  {
    printf '%s\n' '<?xml version="1.0" encoding="UTF-8"?>'
    printf '%s\n' '<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"'
    printf '%s\n' '          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    printf '%s\n' '          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">'
    printf '%s\n' '  <proxies>'
    if [ -n "$HTTP_PROXY_VALUE" ]; then
      proxy_endpoint="${HTTP_PROXY_VALUE#*://}"
      proxy_endpoint="${proxy_endpoint#*@}"
      proxy_endpoint="${proxy_endpoint%%/*}"
      proxy_host="${proxy_endpoint%:*}"
      proxy_port="${proxy_endpoint##*:}"
      printf '%s\n' '    <proxy>'
      printf '%s\n' '      <id>env-http</id>'
      printf '%s\n' '      <active>true</active>'
      printf '%s\n' '      <protocol>http</protocol>'
      printf '%s\n' "      <host>$proxy_host</host>"
      printf '%s\n' "      <port>$proxy_port</port>"
      if [ -n "$non_proxy_hosts" ]; then
        printf '%s\n' "      <nonProxyHosts>$non_proxy_hosts</nonProxyHosts>"
      fi
      printf '%s\n' '    </proxy>'
    fi
    if [ -n "$HTTPS_PROXY_VALUE" ]; then
      proxy_endpoint="${HTTPS_PROXY_VALUE#*://}"
      proxy_endpoint="${proxy_endpoint#*@}"
      proxy_endpoint="${proxy_endpoint%%/*}"
      proxy_host="${proxy_endpoint%:*}"
      proxy_port="${proxy_endpoint##*:}"
      printf '%s\n' '    <proxy>'
      printf '%s\n' '      <id>env-https</id>'
      printf '%s\n' '      <active>true</active>'
      printf '%s\n' '      <protocol>https</protocol>'
      printf '%s\n' "      <host>$proxy_host</host>"
      printf '%s\n' "      <port>$proxy_port</port>"
      if [ -n "$non_proxy_hosts" ]; then
        printf '%s\n' "      <nonProxyHosts>$non_proxy_hosts</nonProxyHosts>"
      fi
      printf '%s\n' '    </proxy>'
    fi
    printf '%s\n' '  </proxies>'
    printf '%s\n' '</settings>'
  } > "$GENERATED_SETTINGS"
fi

if [ ! -x "$maven_home/bin/mvn" ]; then
  mkdir -p "$distribution_dir"
  archive_path="$distribution_dir/$archive_name"
  checksum_path="$archive_path.sha512"

  echo "Downloading Maven from $distribution_url" >&2
  download "$distribution_url" "$archive_path"
  if download "$distribution_url.sha512" "$checksum_path"; then
    expected_checksum="$(awk '{ print $1; exit }' "$checksum_path")"
    actual_checksum="$(shasum -a 512 "$archive_path" | awk '{ print $1; exit }')"
    if [ "$expected_checksum" != "$actual_checksum" ]; then
      echo "Checksum verification failed for $archive_name" >&2
      echo "Expected: $expected_checksum" >&2
      echo "Actual:   $actual_checksum" >&2
      exit 1
    fi
  else
    echo "Warning: checksum download failed; continuing without checksum verification." >&2
  fi

  tar -xzf "$archive_path" -C "$distribution_dir"
fi

if [ -n "$GENERATED_SETTINGS" ]; then
  exec "$maven_home/bin/mvn" --settings "$GENERATED_SETTINGS" "$@"
fi

exec "$maven_home/bin/mvn" "$@"
