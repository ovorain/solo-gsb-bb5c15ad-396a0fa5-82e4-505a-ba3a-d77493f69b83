"""FastAPI 入口：依赖感知的任务租约 API。

启动: uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import Response, JSONResponse

from . import service
from .db import init_db
from .schemas import (
    ClaimIn,
    CompleteIn,
    DAGSubmitIn,
    HeartbeatIn,
    SubscriptionAckIn,
    SubscriptionIn,
    SubscriptionPollIn,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Dependency-Aware Task Lease API",
    version="1.0.0",
    description="提交 DAG、原子领取前置已完成的任务、基于租约令牌回报结果。",
    lifespan=lifespan,
)


@app.exception_handler(service.APIError)
async def api_error_handler(_request: Request, exc: service.APIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message, "details": exc.details}},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/dags", status_code=201)
def submit_dag(body: DAGSubmitIn) -> dict:
    return service.submit_dag(body.model_dump())


@app.get("/dags/{dag_id}")
def get_dag(dag_id: str) -> dict:
    return service.get_dag(dag_id)


@app.get("/dags/{dag_id}/analysis")
def analyze_dag(dag_id: str) -> dict:
    return service.analyze_dag(dag_id)


@app.post("/tasks/claim")
def claim_tasks(body: ClaimIn) -> dict:
    return service.claim_tasks(body.worker_id, body.dag_id, body.limit)


@app.get("/tasks")
def list_tasks(dag_id: str | None = None, state: str | None = None) -> dict:
    return service.list_tasks(dag_id, state)


@app.get("/events")
def list_events(
    after: int = Query(0, ge=0, description="只返回 sequence 大于该值的事件"),
    limit: int = Query(50, ge=1, le=100, description="本页最多返回的事件数（1~100）"),
    dag_id: str | None = Query(None, description="可选：只返回该 DAG 的事件"),
) -> dict:
    return service.list_events(after, limit, dag_id)


# ------------------------------------------------------- 可靠事件订阅

@app.post(
    "/subscriptions",
    status_code=201,
    summary="创建事件订阅",
    responses={
        201: {"description": "订阅已创建并保存 committed_sequence；可选按 dag_id 过滤"},
    },
)
def create_subscription(body: SubscriptionIn) -> dict:
    return service.create_subscription(body.model_dump())


@app.get("/subscriptions", summary="订阅列表")
def list_subscriptions(dag_id: str | None = None) -> dict:
    return service.list_subscriptions(dag_id)


@app.get("/subscriptions/{subscription_id}", summary="订阅详情（含在途批次状态）")
def get_subscription(subscription_id: str) -> dict:
    return service.get_subscription(subscription_id)


@app.post(
    "/subscriptions/{subscription_id}/poll",
    summary="拉取（或在租约内原样回放）下一批事件",
    responses={
        200: {"description": "返回下一批事件与随机 delivery_token；租约未过期时重复 poll 原样回放同一批次"},
        404: {"description": "订阅不存在（subscription_not_found）"},
    },
)
def poll_subscription(subscription_id: str, body: SubscriptionPollIn):
    result = service.poll_subscription(subscription_id, body.model_dump())
    if isinstance(result, service.ReplayedResponse):
        # 租约内重复 poll：逐字节回放首次投递保存的响应体（同令牌、同事件集）
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
    return result


@app.post(
    "/subscriptions/{subscription_id}/ack",
    summary="确认当前批次，把游标推进到该批最后一条 sequence",
    responses={
        200: {"description": "游标已推进到当前批次最后一条 sequence"},
        404: {"description": "订阅不存在（subscription_not_found）"},
        409: {"description": "无在途批次（no_pending_batch）或令牌旧/错误（invalid_delivery_token）"},
    },
)
def ack_subscription(subscription_id: str, body: SubscriptionAckIn) -> dict:
    return service.ack_subscription(subscription_id, body.model_dump())


@app.get("/tasks/{task_id}")
def get_task(task_id: str) -> dict:
    return service.get_task(task_id)


@app.post("/tasks/{task_id}/heartbeat")
def heartbeat(task_id: str, body: HeartbeatIn) -> dict:
    return service.heartbeat(task_id, body.model_dump())


@app.post("/tasks/{task_id}/complete")
def complete_task(task_id: str, body: CompleteIn):
    result = service.complete_task(task_id, body.model_dump())
    if isinstance(result, service.ReplayedResponse):
        # 幂等命中：逐字节回放首次有效回报保存的状态码与响应体
        return Response(
            content=result.body,
            status_code=result.status_code,
            media_type="application/json",
        )
    return result


@app.post(
    "/tasks/{task_id}/resume",
    summary="恢复最终失败的任务",
    responses={
        200: {"description": "已恢复为 pending；前置条件满足的被阻塞后继已在同事务内连锁解锁"},
        404: {"description": "任务不存在（task_not_found）"},
        409: {"description": "任务不是最终失败状态（task_not_failed）；重复恢复同样返回 409 且不新增事件"},
    },
)
def resume_task(task_id: str) -> dict:
    return service.resume_task(task_id)


@app.post(
    "/tasks/{task_id}/cancel",
    summary="取消任务",
    responses={
        200: {"description": "已置为 canceled；running 任务的当前 attempt 被终止、租约被清除，未结束后继在同事务内连锁取消"},
        404: {"description": "任务不存在（task_not_found）"},
        409: {"description": "任务不是 pending/running/blocked（task_not_cancelable）；重复取消同样返回 409 且不新增事件"},
    },
)
def cancel_task(task_id: str) -> dict:
    return service.cancel_task(task_id)
