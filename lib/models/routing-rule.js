'use strict';

class RoutingRule {
  constructor(config) {
    this.condition = config.Condition;
    this.redirect = config.Redirect;
    this.statusCode = (this.redirect && this.redirect.HttpRedirectCode) || 301;
  }

  getRedirectLocation(Key, defaults) {
    let redirectKey = Key;

    if (this.redirect.ReplaceKeyPrefixWith) {
      redirectKey = Key.replace(
        (this.condition && this.condition.KeyPrefixEquals) || /^/,
        this.redirect.ReplaceKeyPrefixWith,
      );
    } else if (this.redirect.ReplaceKeyWith) {
      redirectKey = this.redirect.ReplaceKeyWith;
    }

    const protocol = this.redirect.Protocol || defaults.protocol;
    const hostName = this.redirect.HostName || defaults.hostname;

    return `${protocol}://${hostName}/${redirectKey}`;
  }

  shouldRedirect(Key, statusCode) {
    if (!this.condition) {
      return true;
    }

    if (
      this.condition.KeyPrefixEquals &&
      this.condition.HttpErrorCodeReturnedEquals
    ) {
      return (
        Key.startsWith(this.condition.KeyPrefixEquals) &&
        this.condition.HttpErrorCodeReturnedEquals === statusCode
      );
    }

    if (this.condition.KeyPrefixEquals) {
      return Key.startsWith(this.condition.KeyPrefixEquals);
    }

    if (this.condition.HttpErrorCodeReturnedEquals) {
      return this.condition.HttpErrorCodeReturnedEquals === statusCode;
    }

    return false;
  }
}

module.exports = RoutingRule;
