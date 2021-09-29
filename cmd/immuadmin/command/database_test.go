package immuadmin

/*
func TestDatabaseList(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(dialOptions).WithPasswordReader(pr)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}

	md := metadata.Pairs("authorization", token.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
	}

	cmd, _ := cmdl.NewCmd()
	cmdl.database(cmd)
	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	cmdlist := cmd.Commands()[0].Commands()[1]
	cmdlist.PersistentPreRunE = nil

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "list"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "defaultdb") {
		t.Fatal(err)
	}
}

func TestDatabaseCreate(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(dialOptions).WithPasswordReader(pr)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}

	md := metadata.Pairs("authorization", token.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
	}

	cmd, _ := cmdl.NewCmd()

	cmdl.database(cmd)
	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	cmdlist := cmd.Commands()[0].Commands()[0]
	cmdlist.PersistentPreRunE = nil

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "create", "mynewdb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "database successfully created") {
		t.Fatal(string(msg))
	}
}
*/
