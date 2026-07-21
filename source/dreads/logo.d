module dreads.logo;

/// The banner uses 24-bit ANSI/VT escapes and UTF-8 Braille art. POSIX terminals
/// handle both; a Windows console needs two things turned on first, the same way
/// npm/gh/cargo do it — otherwise the escapes print raw and the Braille mojibakes:
///   - SetConsoleOutputCP(CP_UTF8): render the multibyte Braille, not codepage garbage.
///   - ENABLE_VIRTUAL_TERMINAL_PROCESSING (Windows 10+): interpret the colour escapes.
/// No-op on POSIX and when stdout is not a console (redirect/pipe): GetConsoleMode
/// fails and we simply skip (the escapes still go to the file, exactly as on POSIX).
void enableAnsi()
{
    version (Windows)
    {
        import core.sys.windows.windows : GetStdHandle, STD_OUTPUT_HANDLE,
            GetConsoleMode, SetConsoleMode, SetConsoleOutputCP, DWORD,
            INVALID_HANDLE_VALUE;

        enum CP_UTF8 = 65_001;
        enum ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004;
        SetConsoleOutputCP(CP_UTF8);
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