<<<<<<< 1b62b6785441f46c57a8cfdd3178535841e59986
# emqx-plugin-kafka
emqx kafka plugin
=======

emqx-plugin-template
====================

This is a template plugin for the EMQ X broker. And you can see [Plugin Development Guide](https://developer.emqx.io/docs/emq/v3/en/plugins.html#plugin-development-guide) to learning how to use it.

Plugin Config
-------------

Each plugin should have a 'etc/{plugin_name}.conf|config' file to store application config.

Authentication and ACL
----------------------

```
emqx:hook('client.authenticate', fun ?MODULE:on_client_authenticate/2, [Env]).
emqx:hook('client.check_acl', fun ?MODULE:on_client_check_acl/5, [Env]).
```

Plugin and Hooks
-----------------

[Plugin Design](https://developer.emqx.io/docs/emq/v3/en/design.html#plugin-design)

[Hooks Design](https://developer.emqx.io/docs/emq/v3/en/design.html#hooks-design)

License
-------

Apache License Version 2.0

Author
------

EMQ X Team.
>>>>>>> init commit templete
