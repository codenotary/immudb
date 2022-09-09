from immudb import ImmudbClient, exceptions
import sys

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)


def main():
    client = ImmudbClient(URL)
    try:
        # database parameter is optional
        client.login(LOGIN, PASSWORD, database=DB)

        tx2 = client.verifiedTxById(2)
        print(tx2)

        tx3 = client.verifiedTxById(3)
        print(tx3)

        tx5 = client.verifiedTxById(5)
        print(tx5)

        tx2faked = client.verifiedTxById(2)
        print(tx2faked)

        if tx2 != tx2faked:
            print("Client library is vulnerable, following distinct key sets retrieved for TX 2: {} / {}".format(tx2, tx2faked))
            sys.exit(1)

        print("Suspicious data received from the server, are you using fake server?")
        sys.exit(1)

    except exceptions.ErrCorruptedData as e:
        print("Successfully detected invalid proof")

    finally:
        client.logout()


if __name__ == "__main__":
    main()
