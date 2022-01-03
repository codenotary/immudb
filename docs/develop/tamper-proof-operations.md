# Tamper-proof operations

### State management <a href="#state-management" id="state-management"></a>

It's the responsibility of the immudb client to track the server state. That way it can check each verified read or write operation against a trusted state.

#### Verify state signature <a href="#verify-state-signature" id="verify-state-signature"></a>

If `immudb` is launched with a private signing key, each signed request can be verified with the public key. In this way the identity of the server can be proven. Check state signature to see how to generate a valid key.

### Tamperproof reading and writing <a href="#tamperproof-reading-and-writing" id="tamperproof-reading-and-writing"></a>

You can read and write records securely using a built-in cryptographic verification.

#### Verified get and set <a href="#verified-get-and-set" id="verified-get-and-set"></a>

The client implements the mathematical validations, while your application uses a traditional read or write function.
