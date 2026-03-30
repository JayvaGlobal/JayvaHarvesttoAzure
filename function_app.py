import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def harvest_time_entries_incremental(mytimer: func.TimerRequest) -> None:
    print("Timer trigger loaded and executed.")
