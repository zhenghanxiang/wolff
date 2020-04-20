wolff
========

An EMQ X plugin

##### wolff.conf

```properties
wolff.rule.client.connected.1     = {"action": "on_client_connected"}
wolff.rule.client.disconnected.1  = {"action": "on_client_disconnected"}
wolff.rule.client.subscribe.1     = {"action": "on_client_subscribe"}
wolff.rule.client.unsubscribe.1   = {"action": "on_client_unsubscribe"}
wolff.rule.session.subscribed.1   = {"action": "on_session_subscribed"}
wolff.rule.session.unsubscribed.1 = {"action": "on_session_unsubscribed"}
wolff.rule.message.publish.1      = {"action": "on_message_publish"}
wolff.rule.message.delivered.1    = {"action": "on_message_delivered"}
wolff.rule.message.acked.1        = {"action": "on_message_acked"}
```

License
-------

Apache License Version 2.0

Author
------

Contributors
------------

