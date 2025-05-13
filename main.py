import multiprocessing
from multiprocessing import Process, Semaphore, Manager
import sys

# GLOBALS
ACCOUNT_FILE = "accounts.txt"
TRANSACTION_FILE = "transactions.txt"

#1. Read accounts and create shared dictionary
def read_accounts():
    manager = Manager()
    accounts = manager.dict()
    with open(ACCOUNT_FILE, "r") as f:
        for line in f:
            acc_id, balance = map(int, line.strip().split())
            accounts[acc_id] = balance
    return accounts

#2. Read transactions
def read_transactions():
    transactions = []
    with open(TRANSACTION_FILE, "r") as f:
        for line in f:
            parts = line.strip().split()
            t_type = parts[0]
            if t_type == "deposit" or t_type == "withdraw":
                amount = int(parts[1])
                acc_id = int(parts[2])
                transactions.append((t_type, amount, acc_id))
            elif t_type == "transfer":
                amount = int(parts[1])
                from_id = int(parts[2])
                to_id = int(parts[3])
                transactions.append((t_type, amount, from_id, to_id))
    return transactions

# Account transactions
def deposit(accounts, semaphores, amount, acc_id, result_queue, txn_id):
    with semaphores[acc_id]:
        accounts[acc_id] += amount
        result_queue.put((txn_id, f"Deposit {amount} to Account {acc_id} (Success)"))
        sys.exit(0)

def withdraw(accounts, semaphores, amount, acc_id, result_queue, txn_id):
    with semaphores[acc_id]:
        if accounts[acc_id] >= amount:
            accounts[acc_id] -= amount
            result_queue.put((txn_id, f"Withdraw {amount} from Account {acc_id} (Success)"))
            sys.exit(0)
        else:
            result_queue.put((txn_id, f"Withdraw {amount} from Account {acc_id} (Failed)"))
            sys.exit(-1)

def transfer(accounts, semaphores, amount, from_id, to_id, result_queue, txn_id):
    first, second = sorted([from_id, to_id])
    with semaphores[first], semaphores[second]:
        if accounts[from_id] >= amount:
            accounts[from_id] -= amount
            accounts[to_id] += amount
            result_queue.put((txn_id, f"Transfer {amount} from Account {from_id} to Account {to_id} (Success)"))
            sys.exit(0)
        else:
            result_queue.put((txn_id, f"Transfer {amount} from Account {from_id} to Account {to_id} (Failed)"))
            sys.exit(-1)

def main():
    accounts = read_accounts()
    transactions = read_transactions()
    result_queue = multiprocessing.Queue()
    semaphores = {acc_id: Semaphore(1) for acc_id in accounts.keys()}

    failed_txns = []
    all_results = []

    # 1. Try all operations once in order
    for txn_id, txn in enumerate(transactions):
        t_type = txn[0]
        if t_type == "deposit":
            p = Process(target=deposit, args=(accounts, semaphores, txn[1], txn[2], result_queue, txn_id))
        elif t_type == "withdraw":
            p = Process(target=withdraw, args=(accounts, semaphores, txn[1], txn[2], result_queue, txn_id))
        elif t_type == "transfer":
            p = Process(target=transfer, args=(accounts, semaphores, txn[1], txn[2], txn[3], result_queue, txn_id))
        else:
            continue

        p.start()
        p.join()

        while not result_queue.empty():
            all_results.append(result_queue.get())

        if p.exitcode != 0:
            failed_txns.append((txn_id, txn, t_type))

    # 2. After all the operations are done, try again the ones that failed
    for txn_id, txn, t_type in failed_txns:
        print(f"Retrying transaction {txn_id}...")
        if t_type == "deposit":
            p_retry = Process(target=deposit, args=(accounts, semaphores, txn[1], txn[2], result_queue, txn_id))
        elif t_type == "withdraw":
            p_retry = Process(target=withdraw, args=(accounts, semaphores, txn[1], txn[2], result_queue, txn_id))
        elif t_type == "transfer":
            p_retry = Process(target=transfer, args=(accounts, semaphores, txn[1], txn[2], txn[3], result_queue, txn_id))
        else:
            continue
        p_retry.start()
        p_retry.join()

        while not result_queue.empty():
            all_results.append(result_queue.get())

    print("\nFinal account balances:")
    for acc_id in sorted(accounts.keys()):
        print(f"Account {acc_id}: {accounts[acc_id]}")

    # Write transaction logs to file
    with open("transaction_log.txt", "w") as f:
        f.write("Transaction Log:\n")
        for txn_id, log in sorted(all_results, key=lambda x: (x[0], log_order_key(x[1]))):
            f.write(f"Transaction {txn_id}: {log}\n")

    print("\nTransactions:")
    for txn_id, log in all_results:
        print(f"Transaction {txn_id}: {log}")

def log_order_key(log_msg):
    # "Failed" logs first, "Success" logs later
    return 0 if "Failed" in log_msg else 1

if __name__ == "__main__":
    main()
