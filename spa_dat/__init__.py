"""SPA Distributed Application Tools package."""

import asyncio
import platform

# Windows should use the selector event loop, it supports all necessary features for this library.
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
