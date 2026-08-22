version (unittest)
{
}
else
{
    int main(string[] args)
    {
        import core.stdc.stdio : fwrite, stdout;

        import dreads.config : gConfig, loadConfig, applyDirective;
        import dreads.logo : logo, enableAnsi;
        import dreads.server : runServer;

        import std.algorithm : startsWith;
        import std.conv : to, ConvException;
        import std.file : exists;
        import std.stdio : stderr;
        import std.string : indexOf;

        // redis/valkey-compatible CLI: a positional non-flag arg is either the
        // config file (an existing path) or the port (numeric); every config
        // directive is also settable as `--<directive> <value>` (dashes kept,
        // e.g. `--maxmemory 256mb`, `--maxmemory-policy allkeys-lru`). Processed
        // left-to-right so later args override earlier — a `--flag` after the
        // config file overrides the file, exactly like redis-server.
        string cliLock; // --lockfile= : a CLI-only knob, NOT a config directive

        // DREADS_<DIRECTIVE>=value — the environment half of the redis-style
        // surface (see envToDirective). Applied BEFORE the command line, so a
        // flag or a conf file keeps the last word: a container image's CMD can
        // still override the stack's environment, which is the precedence an
        // operator already expects from redis-server itself.
        {
            import dreads.config : envToDirective;
            import std.process : environment;

            foreach (name, value; environment.toAA)
            {
                auto directive = envToDirective(name);
                if (directive is null)
                    continue;
                if (directive == "lockfile")
                {
                    stderr.writeln("dreads: DREADS_LOCKFILE is ignored; "
                        ~ "--lockfile is CLI-only");
                    continue;
                }
                // Same convenience as the flag: a non-boolean value is the
                // AOF filename, turning the AOF on and naming it at once.
                if (directive == "appendonly" && value != "yes" && value != "no")
                {
                    gConfig.appendonly = true;
                    gConfig.appendfilename = value;
                    continue;
                }
                if (!applyDirective(directive, value, gConfig))
                    stderr.writeln("dreads: ignoring unknown/invalid env: ",
                        name, "=", value);
            }
        }

        auto argv = args[1 .. $];
        for (size_t i = 0; i < argv.length; i++)
        {
            auto arg = argv[i];
            if (arg.startsWith("--"))
            {
                auto b = arg[2 .. $];
                string name, value;
                immutable eq = b.indexOf('=');
                if (eq >= 0)
                {
                    name = b[0 .. eq];
                    value = b[eq + 1 .. $];
                }
                else
                {
                    name = b;
                    // value = the next arg, unless it's another flag / absent
                    // (a bare `--appendonly` then means the boolean `yes`)
                    if (i + 1 < argv.length && !argv[i + 1].startsWith("--"))
                        value = argv[++i];
                    else
                        value = "yes";
                }
                if (name == "lockfile") // CLI-only, not a directive
                {
                    cliLock = value;
                    continue;
                }
                // `--appendonly <path>` convenience: a non-boolean value is the
                // AOF filename (turns it on + names it), matching the old flag.
                if (name == "appendonly" && value != "yes" && value != "no")
                {
                    gConfig.appendonly = true;
                    gConfig.appendfilename = value;
                    continue;
                }
                if (!applyDirective(name, value, gConfig))
                    stderr.writeln("dreads: ignoring unknown/invalid option: --", name, " ", value);
            }
            else
            {
                // positional: numeric => port, existing file => config file
                try
                    gConfig.port = arg.to!ushort;
                catch (ConvException)
                {
                    if (arg.exists)
                    {
                        if (!loadConfig(arg, gConfig,
                                (line) { stderr.writeln("dreads: ignoring config line: ", line); }))
                        {
                            stderr.writeln("dreads: cannot read config: ", arg);
                            return 1;
                        }
                    }
                    else
                    {
                        stderr.writeln(
                            "usage: dreads [conf-file] [port] [--<directive> <value> ...] [--lockfile=path]");
                        return 1;
                    }
                }
            }
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

        enableAnsi(); // Windows: interpret the banner's ANSI escapes (no-op on POSIX)
        fwrite(logo.ptr, 1, logo.length, stdout);

        // The data plane (malloc'd arenas) and the Raft path (automem malloc
        // vectors, pooled pending slots, malloc log) are GC-FREE per operation
        // by construction, so the collector is driven only by the CONTROL plane
        // (per-connection objects, AAs) — infrequent relative to requests. We
        // therefore leave the GC ENABLED: it must run to reclaim those control-
        // plane allocations. A previous global GC.disable() here was a bug — it
        // never collected the per-connection objects, so connection open/close
        // churn leaked ~8-9 KB/conn forever (a slow memory-exhaustion vector,
        // confirmed by adversarial connection-churn fuzzing). GC-free hot paths
        // stay GC-free regardless; only the GC-normal places get collected.

        return runServer(gConfig.port, gConfig.appendonly ? gConfig.appendfilename : null, cliLock);
    }
}
