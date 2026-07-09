version (unittest)
{
}
else
{
    int main(string[] args)
    {
        import core.memory : GC;
        import core.stdc.stdio : fwrite, stdout;

        import dreads.config : gConfig, loadConfig;
        import dreads.logo : logo;
        import dreads.server : runServer;

        import std.algorithm : startsWith;
        import std.conv : to, ConvException;
        import std.file : exists;
        import std.stdio : stderr;

        // like redis-server: a non-flag, non-numeric argument is the config file
        string confPath;
        string cliAof;
        ushort cliPort = 0;
        foreach (arg; args[1 .. $])
        {
            if (arg == "--appendonly")
                cliAof = "dreads.aof";
            else if (arg.startsWith("--appendonly="))
                cliAof = arg["--appendonly=".length .. $];
            else
            {
                try
                    cliPort = arg.to!ushort;
                catch (ConvException)
                {
                    if (arg.exists)
                        confPath = arg;
                    else
                    {
                        stderr.writeln("usage: dreads [conf-file] [port] [--appendonly[=path]]");
                        return 1;
                    }
                }
            }
        }

        if (confPath.length && !loadConfig(confPath, gConfig,
                (line) { stderr.writeln("dreads: ignoring config line: ", line); }))
        {
            stderr.writeln("dreads: cannot read config: ", confPath);
            return 1;
        }
        // CLI overrides the file
        if (cliPort)
            gConfig.port = cliPort;
        if (cliAof.length)
        {
            gConfig.appendonly = true;
            gConfig.appendfilename = cliAof;
        }
        if (gConfig.dir.length)
        {
            import std.file : chdir;

            try
                gConfig.dir.chdir;
            catch (Exception)
            {
                stderr.writeln("dreads: cannot chdir to ", gConfig.dir);
                return 1;
            }
        }

        fwrite(logo.ptr, 1, logo.length, stdout);

        // Neither the data plane (malloc'd arenas) nor the Raft path (automem
        // malloc vectors, pooled pending slots, malloc log) allocates on the GC
        // heap per operation, so disabling the collector is safe under
        // sustained load: vibe-core's own allocations are one-time / per
        // connection (bounded), not per request. This guarantees no GC pauses.
        GC.disable();

        return runServer(gConfig.port, gConfig.appendonly ? gConfig.appendfilename : null);
    }
}
