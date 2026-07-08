version (unittest)
{
}
else
{
    int main(string[] args)
    {
        import core.memory : GC;
        import core.stdc.stdio : fwrite, stdout;

        import dreads.logo : logo;
        import dreads.server : runServer;

        import std.algorithm : startsWith;
        import std.conv : to, ConvException;
        import std.stdio : stderr;

        ushort port = 6379;
        string aofPath = null;
        foreach (arg; args[1 .. $])
        {
            if (arg == "--appendonly")
                aofPath = "dreads.aof";
            else if (arg.startsWith("--appendonly="))
                aofPath = arg["--appendonly=".length .. $];
            else
            {
                try
                    port = arg.to!ushort;
                catch (ConvException)
                {
                    stderr.writeln("usage: dreads [port] [--appendonly[=path]]");
                    return 1;
                }
            }
        }

        fwrite(logo.ptr, 1, logo.length, stdout);

        // The data plane never allocates on the GC heap; disabling the
        // collector guarantees vibe-core's startup allocations never pause us.
        GC.disable();

        return runServer(port, aofPath);
    }
}
