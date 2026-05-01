#!/usr/bin/env python3

import json
import sys
import traceback

try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, OrderArgs, OrderType, PostOrdersArgs
    SDK_FLAVOR = "v2"
except ImportError as exc:  # pragma: no cover - deployment-time dependency check
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, PostOrdersArgs
        SDK_FLAVOR = "legacy"
    except ImportError:
        print(
            "missing dependency: install py-clob-client-v2 (preferred) or py-clob-client into the configured helper interpreter",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        raise


def build_client(payload: dict) -> ClobClient:
    creds = ApiCreds(
        api_key=payload["creds"]["api_key"],
        api_secret=payload["creds"]["api_secret"],
        api_passphrase=payload["creds"]["api_passphrase"],
    )
    return ClobClient(
        payload["host"],
        chain_id=int(payload["chain_id"]),
        key=payload["private_key"],
        creds=creds,
        signature_type=int(payload["signature_type"]),
        funder=payload["funder"],
    )


def parse_order_type(value: str):
    try:
        return getattr(OrderType, value)
    except AttributeError as exc:
        raise ValueError(f"unsupported order type: {value}") from exc


def post_orders(payload: dict) -> list[dict]:
    client = build_client(payload)
    requests = []
    post_only = None
    for item in payload["orders"]:
        if SDK_FLAVOR == "v2":
            order_args = OrderArgs(
                token_id=item["token_id"],
                price=float(item["price"]),
                size=float(item["size"]),
                side=item["side"],
                expiration=int(item["expiration"]),
                builder_code=item.get("builder_code", "0x" + "0" * 64),
                metadata=item.get("metadata", "0x" + "0" * 64),
            )
        else:
            order_args = OrderArgs(
                token_id=item["token_id"],
                price=float(item["price"]),
                size=float(item["size"]),
                side=item["side"],
                expiration=int(item["expiration"]),
                fee_rate_bps=0,
                nonce=0,
                taker="0x0000000000000000000000000000000000000000",
            )
        signed = client.create_order(order_args)
        post_only_value = bool(item["post_only"])
        post_only = post_only_value if post_only is None else post_only
        if post_only != post_only_value:
            raise ValueError("mixed post_only batch is not supported by clob helper")

        requests.append(
            PostOrdersArgs(
                order=signed,
                orderType=parse_order_type(item["order_type"]),
            )
        )
    return client.post_orders(requests, post_only=bool(post_only))


def cancel_orders(payload: dict) -> dict:
    client = build_client(payload)
    return client.cancel_orders(payload["order_ids"])


def get_open_orders(payload: dict) -> list[dict]:
    client = build_client(payload)
    market = payload.get("market")
    asset_id = payload.get("asset_id")

    if SDK_FLAVOR == "v2":
        if market:
            return client.get_orders(market=market)
        if asset_id:
            return client.get_orders(asset_id=asset_id)
        return client.get_orders()

    if market:
        return client.get_orders(market=market)
    if asset_id:
        return client.get_orders(asset_id=asset_id)
    return client.get_orders()


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: clob_helper.py <post-orders|cancel-orders|get-open-orders>", file=sys.stderr)
        return 2

    action = sys.argv[1]
    payload = json.load(sys.stdin)

    if action == "post-orders":
        result = post_orders(payload)
    elif action == "cancel-orders":
        result = cancel_orders(payload)
    elif action == "get-open-orders":
        result = get_open_orders(payload)
    else:
        print(f"unsupported action: {action}", file=sys.stderr)
        return 2

    json.dump(result, sys.stdout, separators=(",", ":"), ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:  # pragma: no cover - surfaced to Rust stderr
        traceback.print_exc(file=sys.stderr)
        raise SystemExit(1)
