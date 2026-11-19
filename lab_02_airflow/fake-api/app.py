from fastapi import FastAPI, HTTPException

app = FastAPI(title="Fake Unstable API")

call_count = 0
FAIL_UNTIL = 2  # falha nas primeiras 2 chamadas, sucesso na 3ª


@app.get('/data')
def data():
    global call_count
    call_count += 1

    if call_count <= FAIL_UNTIL:
        raise HTTPException(status_code=500, detail=f"API indisponível (chamada {call_count})")

    return {"status": "ok", "chamada": call_count, "dados": [1, 2, 3]}


@app.get('/data/always-fail')
def data_always_fail():
    raise HTTPException(status_code=500, detail="API sempre indisponível")


@app.get('/reset')
def reset():
    global call_count
    call_count = 0
    return {"status": "contador resetado"}
