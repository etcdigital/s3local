'use strict';

const { expect } = require('chai');

const RoutingRule = require('../../lib/models/routing-rule');

describe('RoutingRule', () => {
  describe('Condition', () => {
    const matchingKey = 'prefix/Key';
    const nonMatchKey = 'without-prefix/Key';
    const matchingStatusCode = 404;
    const nonMatchStatusCode = 200;

    it('redirects with no condition', () => {
      const rule = new RoutingRule({});

      expect(rule.shouldRedirect('Key', 200)).to.exist;
    });

    it('redirects using only KeyPrefixEquals', () => {
      const rule = new RoutingRule({
        Condition: {
          KeyPrefixEquals: 'prefix',
        },
      });

      expect(rule.shouldRedirect(matchingKey, 200)).to.be.true;
      expect(rule.shouldRedirect(nonMatchKey, 200)).to.be.false;
    });

    it('redirects using only HttpErrorCodeReturnedEquals', () => {
      const rule = new RoutingRule({
        Condition: {
          HttpErrorCodeReturnedEquals: 404,
        },
      });

      expect(rule.shouldRedirect('Key', matchingStatusCode)).to.be.true;
      expect(rule.shouldRedirect('Key', nonMatchStatusCode)).to.be.false;
    });

    it('redirects using both KeyPrefixEquals and HttpErrorCodeReturnedEquals', () => {
      const rule = new RoutingRule({
        Condition: {
          KeyPrefixEquals: 'prefix',
          HttpErrorCodeReturnedEquals: 404,
        },
      });

      expect(rule.shouldRedirect(matchingKey, matchingStatusCode)).to.be.true;
      expect(rule.shouldRedirect(nonMatchKey, matchingStatusCode)).to.be.false;
      expect(rule.shouldRedirect(matchingKey, nonMatchStatusCode)).to.be.false;
      expect(rule.shouldRedirect(nonMatchKey, nonMatchStatusCode)).to.be.false;
    });
  });

  describe('Redirect', () => {
    const defaults = {
      protocol: 'https',
      hostname: 'example.com',
    };

    it('redirects using only HostName', () => {
      const rule = new RoutingRule({
        Redirect: {
          HostName: 'localhost',
        },
      });

      expect(rule.statusCode).to.equal(301);
      expect(rule.getRedirectLocation('Key', defaults)).to.equal(
        'https://localhost/Key',
      );
    });

    it('redirects using only HttpRedirectCode', () => {
      const rule = new RoutingRule({
        Redirect: {
          HttpRedirectCode: 307,
        },
      });

      expect(rule.statusCode).to.equal(307);
      expect(rule.getRedirectLocation('Key', defaults)).to.equal(
        'https://example.com/Key',
      );
    });

    it('redirects using only Protocol', () => {
      const rule = new RoutingRule({
        Redirect: {
          Protocol: 'http',
        },
      });

      expect(rule.statusCode).to.equal(301);
      expect(rule.getRedirectLocation('Key', defaults)).to.equal(
        'http://example.com/Key',
      );
    });

    it('redirects using only ReplaceKeyPrefixWith', () => {
      const rule = new RoutingRule({
        Condition: {
          KeyPrefixEquals: 'prefix',
        },
        Redirect: {
          ReplaceKeyPrefixWith: 'replacement',
        },
      });

      expect(rule.statusCode).to.equal(301);
      expect(rule.getRedirectLocation('prefix/Key', defaults)).to.equal(
        'https://example.com/replacement/Key',
      );
    });

    it('replaces blank prefix with ReplaceKeyPrefixWith', () => {
      const rule = new RoutingRule({
        Redirect: {
          ReplaceKeyPrefixWith: 'replacement/',
        },
      });

      expect(rule.statusCode).to.equal(301);
      expect(rule.getRedirectLocation('prefix/Key', defaults)).to.equal(
        'https://example.com/replacement/prefix/Key',
      );
    });

    it('redirects using only ReplaceKeyWith', () => {
      const rule = new RoutingRule({
        Redirect: {
          ReplaceKeyWith: 'replacement',
        },
      });

      expect(rule.statusCode).to.equal(301);
      expect(rule.getRedirectLocation('Key', defaults)).to.equal(
        'https://example.com/replacement',
      );
    });

    it('redirects using a combination of options', () => {
      const rule = new RoutingRule({
        Condition: {
          KeyPrefixEquals: 'prefix',
        },
        Redirect: {
          Protocol: 'http',
          HttpRedirectCode: 307,
          HostName: 'localhost',
          ReplaceKeyPrefixWith: 'replacement',
        },
      });

      expect(rule.statusCode).to.equal(307);
      expect(rule.getRedirectLocation('prefix/Key', defaults)).to.equal(
        'http://localhost/replacement/Key',
      );
    });
  });
});
