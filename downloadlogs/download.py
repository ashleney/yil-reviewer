# uv: playwright
import asyncio
import pathlib
from playwright.async_api import async_playwright

data_dir = pathlib.Path("playwright_data")
data_dir.mkdir(exist_ok=True)
downloadlogs_script = pathlib.Path("downloadlogs.js").read_text("utf-8")
save_dir = pathlib.Path("downloads")
save_dir.mkdir(exist_ok=True)


async def run_with_blocking_js(urls: list[str]) -> None:
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(data_dir),
            headless=False,
            ignore_default_args=['--enable-automation'],
        )
        page = await context.new_page()

        for url in urls:
            paipu_id = url.split("=")[-1]
            save_path = save_dir / (paipu_id + ".json")
            if save_path.exists():
                print(f"Skip {url}")
                continue
            print(f"Navigating to {url}")
            await page.goto(url, wait_until="networkidle")

            print("evaluating")
            
            async with page.expect_download(timeout=0) as download_info:
                await page.evaluate(downloadlogs_script)
            download = await download_info.value
            path = save_dir / (paipu_id + ".json")
            await download.save_as(str(save_path))
            print(f"JS finished on {url}, file saved to {path}")

        await context.close()


if __name__ == "__main__":
    asyncio.run(run_with_blocking_js(open("urls.txt").read().splitlines()))
