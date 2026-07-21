module dreads.logo;

/// The banner uses 24-bit ANSI/VT escapes. POSIX terminals interpret them; a
/// Windows console does NOT by default, so it would print the escapes as garbage.
/// Turn on ENABLE_VIRTUAL_TERMINAL_PROCESSING (Windows 10+) so the same escapes
/// render like on a POSIX terminal — "something similar" via the Windows API,
/// rather than stripping the colours. No-op on POSIX and when stdout is not a
/// console (redirect/pipe): GetConsoleMode fails and we simply skip.
void enableAnsi()
{
    version (Windows)
    {
        import core.sys.windows.windows : GetStdHandle, STD_OUTPUT_HANDLE,
            GetConsoleMode, SetConsoleMode, DWORD, INVALID_HANDLE_VALUE;

        enum ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004;
        auto h = GetStdHandle(STD_OUTPUT_HANDLE);
        if (h is null || h is INVALID_HANDLE_VALUE)
            return;
        DWORD mode;
        if (GetConsoleMode(h, &mode))
            SetConsoleMode(h, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
    }
}

public string logo = "
\033[38;2;0;70;140m⠀⠀⠀⠀⠀⠀⠀⠀⣀⣤⣴⣶⣾⣿⣿⣿⣿⣷⣶⣶⣤⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⣠⣴⣿⣿⡿⠛⠉⠀⠀⠀⠀⠀⠀⠉⠉⠛⢿⣿⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢀⣴⣿⣿⠟⠋⠀⠀⢀⣀⣤⣤⣤⣤⣀⣀⠀⠀⠀⠀⠈⠛⢿⣿⣷⣤⡀⠀⠀⠀⠀⠀
⠀⣰⣿⣿⠋⠀⠀⠀⣠⣶⣿⣿⠟⠋⣁⣀⠈⠻⣿⣷⣦⡀⠀⠀⠀⠙⣿⣿⣿⣦⣄⠀⠀⠀
⣾⣿⠏⠀⠀⠀⣠⣾⣿⠟⠁⠀⣠⣾⣿⣿⣷⣦⡈⠙⢿⣿⣦⠀⠀⠀⠘⢿⣿⣿⣿⣷⣦⡀
⣿⡏⠀⠀⠀⢀⣿⣿⡏⠀⠀⢸⣿⣿⣿⣿⣿⣿⣿⡇⠀⣿⣿⠀⠀⠀⠀⠈⢻⣿⣿⣿⣿⡇
⠙⠁⠀⠀⠀⠘⠻⣿⣷⣄⠀⠈⠻⣿⣿⣿⣿⣿⡿⠃⣰⣿⡿⠀⠀⠀⠀⠀⠈⠛⠿⠿⠋⠀

\033[38;2;30;180;255m    DREADS \033[1;34m⚡ \033[38;2;80;200;255mDREADS Replicated Event-driven \033[38;2;100;220;255mArena Data Store

\033[38;2;120;240;255m    ⟜ Ultra-light. \033[38;2;100;255;240mThread-isolated DBs.
\033[38;2;80;255;220m    ⟜ Arena memory. \033[38;2;60;255;200mZero-GC overhead.
\033[38;2;40;255;180m    ⟜ Geo indexing. \033[38;2;20;255;160mCustom types. \033[38;2;0;255;120mOne purpose: Speed.\033[0m

";