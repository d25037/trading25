# messaging package¶

# messaging package[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/__init__.py "Jump to source")[¶](index.html#vectorbt.messaging "Permanent link")

Modules for messaging.

* * *

## Sub-modules[¶](index.html#sub-modules "Permanent link")

  * [vectorbt.messaging.telegram](telegram/index.html "vectorbt.messaging.telegram")

## Submodules

### telegram module¶

# telegram module[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py "Jump to source")[¶](index.html#vectorbt.messaging.telegram "Permanent link")

Messaging using `python-telegram-bot`.

* * *

## self_decorator function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L64-L70 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.self_decorator "Permanent link")

    self_decorator(
        func
    )

Pass bot object to func command.

* * *

## send_action function[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L47-L61 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.send_action "Permanent link")

    send_action(
        action
    )

Sends `action` while processing func command.

Suitable only for bound callbacks taking arguments `self`, `update`, `context` and optionally other.

* * *

## LogHandler class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L31-L44 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.LogHandler "Permanent link")

    LogHandler(
        callback,
        pass_update_queue=False,
        pass_job_queue=False,
        pass_user_data=False,
        pass_chat_data=False,
        run_async=False
    )

Handler to log user updates.

**Superclasses**

  * `abc.ABC`
  * `telegram.ext.handler.Handler`
  * `typing.Generic`

* * *

### check_update method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L34-L44 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.LogHandler.check_update "Permanent link")

    LogHandler.check_update(
        update
    )

This method is called to determine if an update should be handled by this handler instance. It should always be overridden.

**Note**

Custom updates types can be handled by the dispatcher. Therefore, an implementation of this method should always check the type of :attr:`update`.

**Args**

update (:obj:`str` | :class:`telegram.Update`): The update to be tested. **Returns**

Either :obj:`None` or :obj:`False` if the update should not be handled. Otherwise an object that will be passed to :meth:`handle_update` and :meth:`collect_additional_context` when the update gets handled.

* * *

## TelegramBot class[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L73-L365 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot "Permanent link")

    TelegramBot(
        giphy_kwargs=None,
        **kwargs
    )

Telegram bot.

See [Extensions – Your first Bot](https://github.com/python-telegram-bot/python-telegram-bot/wiki/Extensions-%E2%80%93-Your-first-Bot).

`**kwargs` are passed to `telegram.ext.updater.Updater` and override settings under `messaging.telegram` in [settings](../../_settings/index.html#vectorbt._settings.settings "vectorbt._settings.settings").

**Usage**

  * Let's extend [TelegramBot](index.html#vectorbt.messaging.telegram.TelegramBot "vectorbt.messaging.telegram.TelegramBot") to track cryptocurrency prices:

    >>> from telegram.ext import CommandHandler
    >>> import ccxt
    >>> import logging
    >>> import vectorbt as vbt
    
    >>> logging.basicConfig(level=logging.INFO)  # enable logging
    
    >>> class MyTelegramBot(vbt.TelegramBot):
    ...     @property
    ...     def custom_handlers(self):
    ...         return (CommandHandler('get', self.get),)
    ...
    ...     @property
    ...     def help_message(self):
    ...         return "Type /get [symbol] [exchange id (optional)] to get the latest price."
    ...
    ...     def get(self, update, context):
    ...         chat_id = update.effective_chat.id
    ...
    ...         if len(context.args) == 1:
    ...             symbol = context.args[0]
    ...             exchange = 'binance'
    ...         elif len(context.args) == 2:
    ...             symbol = context.args[0]
    ...             exchange = context.args[1]
    ...         else:
    ...             self.send_message(chat_id, "This command requires symbol and optionally exchange id.")
    ...             return
    ...         try:
    ...             ticker = getattr(ccxt, exchange)().fetchTicker(symbol)
    ...         except Exception as e:
    ...             self.send_message(chat_id, str(e))
    ...             return
    ...         self.send_message(chat_id, str(ticker['last']))
    
    >>> bot = MyTelegramBot(token='YOUR_TOKEN')
    >>> bot.start()
    INFO:vectorbt.utils.messaging:Initializing bot
    INFO:vectorbt.utils.messaging:Loaded chat ids [447924619]
    INFO:vectorbt.utils.messaging:Running bot vectorbt_bot
    INFO:apscheduler.scheduler:Scheduler started
    INFO:vectorbt.utils.messaging:447924619 - Bot: "I'm back online!"
    INFO:vectorbt.utils.messaging:447924619 - User: "/start"
    INFO:vectorbt.utils.messaging:447924619 - Bot: "Hello!"
    INFO:vectorbt.utils.messaging:447924619 - User: "/help"
    INFO:vectorbt.utils.messaging:447924619 - Bot: "Type /get [symbol] [exchange id (optional)] to get the latest price."
    INFO:vectorbt.utils.messaging:447924619 - User: "/get BTC/USDT"
    INFO:vectorbt.utils.messaging:447924619 - Bot: "55530.55"
    INFO:vectorbt.utils.messaging:447924619 - User: "/get BTC/USD bitmex"
    INFO:vectorbt.utils.messaging:447924619 - Bot: "55509.0"
    INFO:telegram.ext.updater:Received signal 2 (SIGINT), stopping...
    INFO:apscheduler.scheduler:Scheduler has been shut down

**Superclasses**

  * [Configured](../../utils/config/index.html#vectorbt.utils.config.Configured "vectorbt.utils.config.Configured")
  * [Documented](../../utils/docs/index.html#vectorbt.utils.docs.Documented "vectorbt.utils.docs.Documented")
  * [Pickleable](../../utils/config/index.html#vectorbt.utils.config.Pickleable "vectorbt.utils.config.Pickleable")

**Inherited members**

  * [Configured.config](../../utils/config/index.html#vectorbt.utils.config.Configured.config "vectorbt.utils.config.Configured.config")
  * [Configured.copy()](../../utils/config/index.html#vectorbt.utils.config.Configured.copy "vectorbt.utils.config.Configured.copy")
  * [Configured.dumps()](../../utils/config/index.html#vectorbt.utils.config.Pickleable.dumps "vectorbt.utils.config.Configured.dumps")
  * [Configured.loads()](../../utils/config/index.html#vectorbt.utils.config.Pickleable.loads "vectorbt.utils.config.Configured.loads")
  * [Configured.replace()](../../utils/config/index.html#vectorbt.utils.config.Configured.replace "vectorbt.utils.config.Configured.replace")
  * [Configured.to_doc()](../../utils/docs/index.html#vectorbt.utils.docs.Documented.to_doc "vectorbt.utils.config.Configured.to_doc")
  * [Configured.update_config()](../../utils/config/index.html#vectorbt.utils.config.Configured.update_config "vectorbt.utils.config.Configured.update_config")
  * [Configured.writeable_attrs](../../utils/config/index.html#vectorbt.utils.config.Configured.writeable_attrs "vectorbt.utils.config.Configured.writeable_attrs")
  * [Pickleable.load()](../../utils/config/index.html#vectorbt.utils.config.Pickleable.load "vectorbt.utils.config.Configured.load")
  * [Pickleable.save()](../../utils/config/index.html#vectorbt.utils.config.Pickleable.save "vectorbt.utils.config.Configured.save")

* * *

### chat_ids property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L214-L219 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.chat_ids "Permanent link")

Chat ids that ever interacted with this bot.

A chat id is added upon receiving the "/start" command.

* * *

### chat_migration_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L333-L341 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.chat_migration_callback "Permanent link")

    TelegramBot.chat_migration_callback(
        update,
        context
    )

Chat migration callback.

* * *

### custom_handlers property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L207-L212 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.custom_handlers "Permanent link")

Custom handlers to add.

Override to add custom handlers. Order counts.

* * *

### dispatcher property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L197-L200 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.dispatcher "Permanent link")

Dispatcher.

* * *

### error_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L350-L355 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.error_callback "Permanent link")

    TelegramBot.error_callback(
        update,
        context,
        *args
    )

Error callback.

* * *

### help_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L327-L331 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.help_callback "Permanent link")

    TelegramBot.help_callback(
        update,
        context
    )

Help command callback.

* * *

### help_message property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L320-L325 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.help_message "Permanent link")

Message to be sent upon "/help" command.

Override to define your own message.

* * *

### log_handler property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L202-L205 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.log_handler "Permanent link")

Log handler.

* * *

### running property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L362-L365 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.running "Permanent link")

Whether the bot is running.

* * *

### send method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L256-L272 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send "Permanent link")

    TelegramBot.send(
        kind,
        chat_id,
        *args,
        log_msg=None,
        **kwargs
    )

Send message of any kind to `chat_id`.

* * *

### send_giphy method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L289-L295 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send_giphy "Permanent link")

    TelegramBot.send_giphy(
        chat_id,
        text,
        *args,
        giphy_kwargs=None,
        **kwargs
    )

Send GIPHY from text to `chat_id`.

* * *

### send_giphy_to_all method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L297-L303 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send_giphy_to_all "Permanent link")

    TelegramBot.send_giphy_to_all(
        text,
        *args,
        giphy_kwargs=None,
        **kwargs
    )

Send GIPHY from text to all in [TelegramBot.chat_ids](index.html#vectorbt.messaging.telegram.TelegramBot.chat_ids "vectorbt.messaging.telegram.TelegramBot.chat_ids").

* * *

### send_message method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L279-L282 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send_message "Permanent link")

    TelegramBot.send_message(
        chat_id,
        text,
        *args,
        **kwargs
    )

Send text message to `chat_id`.

* * *

### send_message_to_all method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L284-L287 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send_message_to_all "Permanent link")

    TelegramBot.send_message_to_all(
        text,
        *args,
        **kwargs
    )

Send text message to all in [TelegramBot.chat_ids](index.html#vectorbt.messaging.telegram.TelegramBot.chat_ids "vectorbt.messaging.telegram.TelegramBot.chat_ids").

* * *

### send_to_all method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L274-L277 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.send_to_all "Permanent link")

    TelegramBot.send_to_all(
        kind,
        *args,
        **kwargs
    )

Send message of any kind to all in [TelegramBot.chat_ids](index.html#vectorbt.messaging.telegram.TelegramBot.chat_ids "vectorbt.messaging.telegram.TelegramBot.chat_ids").

* * *

### start method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L221-L248 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.start "Permanent link")

    TelegramBot.start(
        in_background=False,
        **kwargs
    )

Start the bot.

`**kwargs` are passed to `telegram.ext.updater.Updater.start_polling` and override settings under `messaging.telegram` in [settings](../../_settings/index.html#vectorbt._settings.settings "vectorbt._settings.settings").

* * *

### start_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L312-L318 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.start_callback "Permanent link")

    TelegramBot.start_callback(
        update,
        context
    )

Start command callback.

* * *

### start_message property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L305-L310 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.start_message "Permanent link")

Message to be sent upon "/start" command.

Override to define your own message.

* * *

### started_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L250-L254 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.started_callback "Permanent link")

    TelegramBot.started_callback()

Callback once the bot has been started.

Override to execute custom commands upon starting the bot.

* * *

### stop method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L357-L360 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.stop "Permanent link")

    TelegramBot.stop()

Stop the bot.

* * *

### unknown_callback method[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L343-L348 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.unknown_callback "Permanent link")

    TelegramBot.unknown_callback(
        update,
        context
    )

Unknown command callback.

* * *

### updater property[](https://github.com/polakowo/vectorbt/blob/8936ddd22b243635d690dd5d033834e62fb31391/vectorbt/messaging/telegram.py#L192-L195 "Jump to source")[¶](index.html#vectorbt.messaging.telegram.TelegramBot.updater "Permanent link")

Updater.

