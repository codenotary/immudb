
# Embedded REST server

## Get Started

```
export IMMUCLIENT_USERNAME=immudb IMMUCLIENT_PASSWORD=immudb
export IMMUCLIENT_USERNAME_BASE64=$(echo -n $IMMUCLIENT_USERNAME | base64)
export IMMUCLIENT_PASSWORD_BASE64=$(echo -n $IMMUCLIENT_PASSWORD | base64)

export TOKEN=$(curl -X POST -d "{\"user\": \"$IMMUCLIENT_USERNAME_BASE64\", \"password\": \"$IMMUCLIENT_PASSWORD_BASE64\"}" http://localhost:9997/v1/immurestproxy/login | jq -r .token)

export KEY=$(echo -n key1 | base64) VAL=$(echo -n val1 | base64)

curl  -X POST -H "authorization: Bearer $TOKEN" -d "{\"KVs\": [{\"key\": \"$KEY\", \"value\": \"$VAL\"}]}" http://localhost:9997/v1/immurestproxy/db/set
```
