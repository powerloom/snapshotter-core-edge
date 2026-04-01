#!/usr/bin/env python3
import json
import sys

def format_payment_info(data):
    """Format payment receipt information"""
    lines = data.strip().split('\n')
    
    print("=" * 80)
    print("PAYMENT RECEIPT")
    print("=" * 80)
    
    json_start_idx = -1
    for idx, line in enumerate(lines):
        if line.startswith('status'):
            status = line.split(' ', 1)[1]
            print(f"Status: {status}")
        elif line.startswith('payment_receipt_header'):
            header = line.split(' ', 1)[1]
            print(f"Receipt Header: {header[:50]}...")
        elif line.startswith('payment_reference_tx'):
            tx = line.split(' ', 1)[1]
            print(f"Transaction: {tx}")
        elif line.strip().startswith('{'):
            json_start_idx = idx
            break
    
    if json_start_idx == -1:
        print("\nNo JSON data found in output")
        return
    
    print()
    
    # Parse the JSON data - join all remaining lines
    json_str = ''.join(lines[json_start_idx:])
    
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        print(f"JSON string length: {len(json_str)}")
        print(f"First 200 chars: {json_str[:200]}")
        print(f"Last 200 chars: {json_str[-200:]}")
        return
    
    print("=" * 80)
    print("TRADE DATA")
    print("=" * 80)
    
    # Epoch info
    epoch = data.get('epoch', {})
    print(f"\nEpoch Range: {epoch.get('begin', 'N/A')} → {epoch.get('end', 'N/A')}")
    
    # Trade details
    trade_data_dict = data.get('tradeData', {})
    total_addresses = len(trade_data_dict)
    total_trades = sum(len(info.get('trades', [])) for info in trade_data_dict.values())
    
    print(f"\nTotal Addresses: {total_addresses}")
    print(f"Total Trades: {total_trades}")
    
    for address, trade_info in trade_data_dict.items():
        print(f"\n{'─' * 80}")
        print(f"Address: {address}")
        print(f"{'─' * 80}")
        
        trades = trade_info.get('trades', [])
        print(f"\nTrades: {len(trades)}")
        
        for idx, trade in enumerate(trades, 1):
            print(f"\n  Trade #{idx}")
            print(f"  {'─' * 76}")
            print(f"  Type: {trade.get('tradeType', 'Unknown')}")
            
            # Log info
            log = trade.get('log', {})
            print(f"\n  Block: {log.get('blockNumber', 'N/A')}")
            print(f"  Transaction: {log.get('transactionHash', 'N/A')}")
            print(f"  Event: {log.get('eventName', 'N/A')}")
            print(f"  Log Index: {log.get('logIndex', 'N/A')}")
            
            # Trade data
            trade_data = trade.get('data', {})
            if trade_data:
                print(f"\n  Trade Details:")
                
                # Format amounts safely
                amount0 = trade_data.get('amount0')
                amount1 = trade_data.get('amount1')
                if amount0 is not None:
                    print(f"    Amount 0: {amount0:,.2f}")
                if amount1 is not None:
                    print(f"    Amount 1: {amount1:,.2f}")
                
                token0_amt = trade_data.get('calculated_token0_amount')
                token1_amt = trade_data.get('calculated_token1_amount')
                if token0_amt is not None:
                    print(f"    Token 0 Amount: {token0_amt:.6f}")
                if token1_amt is not None:
                    print(f"    Token 1 Amount: {token1_amt:.6f}")
                
                usd_amt = trade_data.get('calculated_trade_amount_usd')
                if usd_amt is not None:
                    print(f"    Trade Amount (USD): ${usd_amt:.2f}")
                
                eth_price = trade_data.get('calculated_eth_price')
                if eth_price is not None:
                    print(f"    ETH Price: ${eth_price:,.2f}")
                
                liquidity = trade_data.get('liquidity')
                if liquidity is not None:
                    print(f"    Liquidity: {liquidity:,}")
                
                tick = trade_data.get('tick')
                if tick is not None:
                    print(f"    Tick: {tick:,}")
                
                sender = trade_data.get('sender')
                if sender:
                    print(f"    Sender: {sender}")
                
                recipient = trade_data.get('recipient')
                if recipient:
                    print(f"    Recipient: {recipient}")
                
                timestamp = trade_data.get('block_timestamp')
                if timestamp:
                    print(f"    Timestamp: {timestamp}")
    
    print(f"\n{'=' * 80}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            content = f.read()
    else:
        content = sys.stdin.read()
    
    format_payment_info(content)
