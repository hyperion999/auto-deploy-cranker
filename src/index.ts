/**
 * Auto-Deploy Cranker for Dark Matter
 * 
 * Railway-compatible service that automatically deploys for users with automation enabled.
 * Executes auto_deploy for all active automation accounts.
 * 
 * Optimized for Helius RPC with batch processing and priority fees.
 * 
 * Environment Variables:
 *   SOLANA_RPC_URL    - Solana RPC endpoint (required) - use Helius for best performance
 *   PRIVATE_KEY       - Executor wallet private key as JSON array (required)
 *   PROGRAM_ID        - Dark Matter program ID (required)
 *   DRY_RUN           - Set to "1" for dry run mode (optional)
 *   POLL_INTERVAL_MS  - Polling interval in ms (default: 5000)
 *   BATCH_SIZE        - Number of parallel transactions (default: 10)
 *   PRIORITY_FEE      - Priority fee in microlamports (default: 50000)
 */

// Polyfill for Node.js < 19 (crypto not globally available)
import { webcrypto } from 'crypto';
if (typeof globalThis.crypto === 'undefined') {
  (globalThis as any).crypto = webcrypto;
}

import { 
  Connection, 
  Keypair, 
  PublicKey, 
  SystemProgram,
  LAMPORTS_PER_SOL,
  TransactionInstruction,
  TransactionMessage,
  VersionedTransaction,
  ComputeBudgetProgram
} from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";

// Load environment variables
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const PROGRAM_ID_STR = process.env.PROGRAM_ID;
const DRY_RUN = process.env.DRY_RUN === "1";
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000");
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "10");
const PRIORITY_FEE = parseInt(process.env.PRIORITY_FEE || "50000"); // microlamports

if (!SOLANA_RPC_URL) throw new Error("SOLANA_RPC_URL is required");
if (!PRIVATE_KEY) throw new Error("PRIVATE_KEY is required");
if (!PROGRAM_ID_STR) throw new Error("PROGRAM_ID is required");

const PROGRAM_ID = new PublicKey(PROGRAM_ID_STR);

// Parse private key (supports JSON array format)
function loadKeypair(privateKeyStr: string): Keypair {
  try {
    const parsed = JSON.parse(privateKeyStr);
    return Keypair.fromSecretKey(Uint8Array.from(parsed));
  } catch {
    throw new Error("PRIVATE_KEY must be a JSON array of numbers");
  }
}

// Seeds
const BOARD_SEED = Buffer.from("board");
const CONFIG_SEED = Buffer.from("config");
const ROUND_SEED = Buffer.from("round");
const MINER_SEED = Buffer.from("miner");
const AUTOMATION_SEED = Buffer.from("automation");

const NUM_TILES = 15;
const CHECKPOINT_FEE = 10_000; // 0.00001 SOL

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const toBytes = (value: number) => {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(value));
  return buf;
};

const deriveBoardPda = () =>
  PublicKey.findProgramAddressSync([BOARD_SEED], PROGRAM_ID)[0];

const deriveConfigPda = () =>
  PublicKey.findProgramAddressSync([CONFIG_SEED], PROGRAM_ID)[0];

const deriveRoundPda = (roundId: number) =>
  PublicKey.findProgramAddressSync([ROUND_SEED, toBytes(roundId)], PROGRAM_ID)[0];

const deriveMinerPda = (authority: PublicKey) =>
  PublicKey.findProgramAddressSync([MINER_SEED, authority.toBuffer()], PROGRAM_ID)[0];

const deriveAutomationPda = (authority: PublicKey) =>
  PublicKey.findProgramAddressSync([AUTOMATION_SEED, authority.toBuffer()], PROGRAM_ID)[0];

function decodeBoard(data: Buffer) {
  let offset = 8;
  const roundId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const startSlot = Number(data.readBigUInt64LE(offset)); offset += 8;
  const endSlot = Number(data.readBigUInt64LE(offset));
  return { roundId, startSlot, endSlot };
}

function decodeConfig(data: Buffer) {
  let offset = 8;
  const admin = new PublicKey(data.slice(offset, offset + 32)); offset += 32;
  const feeCollector = new PublicKey(data.slice(offset, offset + 32));
  return { admin, feeCollector };
}

function decodeAutomation(data: Buffer) {
  let offset = 8;
  const authority = new PublicKey(data.slice(offset, offset + 32)); offset += 32;
  const executor = new PublicKey(data.slice(offset, offset + 32)); offset += 32;
  const balance = Number(data.readBigUInt64LE(offset)); offset += 8;
  const amountPerTile = Number(data.readBigUInt64LE(offset)); offset += 8;
  const executorFee = Number(data.readBigUInt64LE(offset)); offset += 8;
  const strategy = data.readUInt8(offset); offset += 1;
  const tileConfig = data.readUInt16LE(offset);
  return { authority, executor, balance, amountPerTile, executorFee, strategy, tileConfig };
}

function decodeMiner(data: Buffer) {
  let offset = 8;
  const authority = new PublicKey(data.slice(offset, offset + 32)); offset += 32;
  offset += NUM_TILES * 8 * 2; // Skip deployed and cumulative arrays
  const roundId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const checkpointId = Number(data.readBigUInt64LE(offset)); offset += 8;
  const checkpointFee = Number(data.readBigUInt64LE(offset));
  return { authority, roundId, checkpointId, checkpointFee };
}

// Get all automation accounts that use this executor
async function getAutomationsForExecutor(
  connection: Connection, 
  executor: PublicKey
): Promise<{ pubkey: PublicKey; data: ReturnType<typeof decodeAutomation> }[]> {
  // Automation account size
  const AUTOMATION_SIZE = 8 + 32 + 32 + 8 + 8 + 8 + 1 + 2 + 1;
  
  const accounts = await connection.getProgramAccounts(PROGRAM_ID, {
    filters: [{ dataSize: AUTOMATION_SIZE }],
  });
  
  const automations: { pubkey: PublicKey; data: ReturnType<typeof decodeAutomation> }[] = [];
  
  for (const account of accounts) {
    try {
      const data = decodeAutomation(account.account.data as Buffer);
      // Only include automations where we are the executor
      if (data.executor.equals(executor) && !data.authority.equals(PublicKey.default)) {
        automations.push({ pubkey: account.pubkey, data });
      }
    } catch {
      // Not a valid automation account, skip
    }
  }
  
  return automations;
}

// Batch fetch miner accounts for eligibility check
async function batchGetMinerAccounts(
  connection: Connection,
  authorities: PublicKey[]
): Promise<Map<string, ReturnType<typeof decodeMiner> | null>> {
  const minerPdas = authorities.map(a => deriveMinerPda(a));
  const minerInfos = await connection.getMultipleAccountsInfo(minerPdas);
  
  const result = new Map<string, ReturnType<typeof decodeMiner> | null>();
  
  for (let i = 0; i < authorities.length; i++) {
    const authority = authorities[i].toBase58();
    const info = minerInfos[i];
    if (info) {
      try {
        result.set(authority, decodeMiner(info.data as Buffer));
      } catch {
        result.set(authority, null);
      }
    } else {
      result.set(authority, null);
    }
  }
  
  return result;
}

function canDeployWithMinerData(
  automation: ReturnType<typeof decodeAutomation>,
  minerData: ReturnType<typeof decodeMiner> | null,
  currentRoundId: number
): boolean {
  // Check if automation has sufficient balance
  const minRequired = automation.amountPerTile + automation.executorFee + CHECKPOINT_FEE;
  if (automation.balance < minRequired) {
    return false;
  }
  
  if (minerData) {
    // If miner participated in a previous round and hasn't checkpointed
    const isNeverCheckpointed = minerData.checkpointId > 1e18;
    if (minerData.roundId < currentRoundId && 
        minerData.checkpointId !== minerData.roundId && 
        !isNeverCheckpointed) {
      return false; // Needs checkpoint first
    }
    // If already deployed this round
    if (minerData.roundId === currentRoundId) {
      return false; // Already deployed
    }
  }
  
  return true;
}

// Build auto-deploy transaction with priority fee
async function buildAutoDeployTx(
  program: anchor.Program,
  connection: Connection,
  executor: Keypair,
  automation: { pubkey: PublicKey; data: ReturnType<typeof decodeAutomation> },
  boardPda: PublicKey,
  configPda: PublicKey,
  feeCollector: PublicKey,
  currentRoundId: number
): Promise<VersionedTransaction> {
  const minerPda = deriveMinerPda(automation.data.authority);
  const roundPda = deriveRoundPda(currentRoundId);
  
  // Build the auto_deploy instruction
  const ix = await program.methods
    .autoDeploy()
    .accounts({
      executor: executor.publicKey,
      authority: automation.data.authority,
      config: configPda,
      board: boardPda,
      round: roundPda,
      miner: minerPda,
      automation: automation.pubkey,
      feeCollector: feeCollector,
      systemProgram: SystemProgram.programId,
    })
    .instruction();
  
  // Add priority fee instructions
  const priorityFeeIx = ComputeBudgetProgram.setComputeUnitPrice({
    microLamports: PRIORITY_FEE,
  });
  
  const computeUnitsIx = ComputeBudgetProgram.setComputeUnitLimit({
    units: 200_000, // auto_deploy should need less than this
  });
  
  // Get recent blockhash
  const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');
  
  // Build versioned transaction
  const messageV0 = new TransactionMessage({
    payerKey: executor.publicKey,
    recentBlockhash: blockhash,
    instructions: [priorityFeeIx, computeUnitsIx, ix],
  }).compileToV0Message();
  
  const tx = new VersionedTransaction(messageV0);
  tx.sign([executor]);
  
  return tx;
}

// Send transaction with retry logic
async function sendTxWithRetry(
  connection: Connection,
  tx: VersionedTransaction,
  authority: string,
  maxRetries: number = 3
): Promise<{ success: boolean; isBuffer: boolean }> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const sig = await connection.sendTransaction(tx, {
        skipPreflight: false,
        maxRetries: 2,
      });
      
      // Wait for confirmation with timeout
      const confirmation = await connection.confirmTransaction({
        signature: sig,
        blockhash: tx.message.recentBlockhash,
        lastValidBlockHeight: (await connection.getLatestBlockhash()).lastValidBlockHeight,
      }, 'confirmed');
      
      if (confirmation.value.err) {
        const errStr = JSON.stringify(confirmation.value.err);
        if (errStr.includes('6008') || errStr.includes('BufferPeriod')) {
          return { success: false, isBuffer: true };
        }
        console.error(`  ✗ Failed for ${authority}...: ${errStr}`);
        return { success: false, isBuffer: false };
      }
      
      console.log(`  ✓ Auto-deployed for ${authority}...`);
      return { success: true, isBuffer: false };
    } catch (err: any) {
      const errMsg = err.message || '';
      
      // Check if it's a buffer period error
      if (errMsg.includes('BufferPeriod') || errMsg.includes('6008')) {
        if (attempt < maxRetries - 1) {
          await sleep(2000);
          continue;
        }
        return { success: false, isBuffer: true };
      }
      
      // Rate limit - wait and retry
      if (errMsg.includes('429') || errMsg.includes('rate') || errMsg.includes('Too many')) {
        console.log(`  Rate limited, waiting 1s...`);
        await sleep(1000);
        continue;
      }
      
      console.error(`  ✗ Failed for ${authority}...: ${errMsg}`);
      return { success: false, isBuffer: false };
    }
  }
  
  return { success: false, isBuffer: false };
}

// Process automations in batches
async function processBatch(
  program: anchor.Program,
  connection: Connection,
  executor: Keypair,
  automations: { pubkey: PublicKey; data: ReturnType<typeof decodeAutomation> }[],
  boardPda: PublicKey,
  configPda: PublicKey,
  feeCollector: PublicKey,
  currentRoundId: number
): Promise<{ deployed: number; errors: number; bufferErrors: number }> {
  let deployed = 0;
  let errors = 0;
  let bufferErrors = 0;
  
  if (DRY_RUN) {
    for (const automation of automations) {
      console.log(`  [DRY RUN] Would auto-deploy for ${automation.data.authority.toBase58().slice(0, 8)}...`);
    }
    return { deployed: automations.length, errors: 0, bufferErrors: 0 };
  }
  
  // Build all transactions first
  const txPromises = automations.map(automation => 
    buildAutoDeployTx(program, connection, executor, automation, boardPda, configPda, feeCollector, currentRoundId)
      .then(tx => ({ tx, automation }))
      .catch(err => {
        console.error(`  ✗ Failed to build tx for ${automation.data.authority.toBase58().slice(0, 8)}...: ${err.message}`);
        return null;
      })
  );
  
  const txResults = await Promise.all(txPromises);
  const validTxs = txResults.filter((r): r is { tx: VersionedTransaction; automation: typeof automations[0] } => r !== null);
  
  // Send all transactions in parallel
  const sendPromises = validTxs.map(({ tx, automation }) => 
    sendTxWithRetry(connection, tx, automation.data.authority.toBase58().slice(0, 8))
  );
  
  const results = await Promise.all(sendPromises);
  
  for (const result of results) {
    if (result.success) {
      deployed++;
    } else if (result.isBuffer) {
      bufferErrors++;
    } else {
      errors++;
    }
  }
  
  // Count build failures as errors
  errors += txResults.filter(r => r === null).length;
  
  return { deployed, errors, bufferErrors };
}

// Wait for buffer period to end
async function waitForBufferEnd(connection: Connection, boardPda: PublicKey): Promise<boolean> {
  const boardInfo = await connection.getAccountInfo(boardPda);
  if (!boardInfo) return false;
  
  const boardData = decodeBoard(boardInfo.data as Buffer);
  const currentSlot = await connection.getSlot();
  
  // If waiting for first deploy (round not started)
  const MAX_U64_APPROX = 1.8e19;
  if (boardData.endSlot > MAX_U64_APPROX) {
    return true; // Can deploy to start the round
  }
  
  // If round ended, can't deploy
  if (currentSlot >= boardData.endSlot) {
    return false;
  }
  
  // If in buffer period, wait for it to end
  if (currentSlot < boardData.startSlot) {
    const slotsToWait = boardData.startSlot - currentSlot;
    const msToWait = Math.min(slotsToWait * 400, 30000); // Max 30 second wait
    console.log(`  Waiting ${Math.ceil(msToWait/1000)}s for buffer period to end...`);
    await sleep(msToWait + 1000); // Add 1 second buffer
    return true;
  }
  
  return true;
}

async function runAutoDeployCycle(
  program: anchor.Program,
  connection: Connection,
  executor: Keypair,
  boardPda: PublicKey,
  configPda: PublicKey,
  feeCollector: PublicKey
): Promise<{ deployed: number; skipped: number; errors: number }> {
  let deployed = 0;
  let skipped = 0;
  let errors = 0;
  
  const boardInfo = await connection.getAccountInfo(boardPda);
  if (!boardInfo) {
    return { deployed: 0, skipped: 0, errors: 0 };
  }
  
  const boardData = decodeBoard(boardInfo.data as Buffer);
  const currentSlot = await connection.getSlot();
  
  // Check if we're in active round (not in buffer period and round started)
  const MAX_U64_APPROX = 1.8e19;
  if (boardData.endSlot > MAX_U64_APPROX) {
    // Round waiting for first deploy - we can trigger it!
  } else if (currentSlot < boardData.startSlot) {
    // Still in buffer period - silently skip
    return { deployed: 0, skipped: 0, errors: 0 };
  } else if (currentSlot >= boardData.endSlot) {
    // Round ended, wait for reset
    return { deployed: 0, skipped: 0, errors: 0 };
  }
  
  // Get all automations for this executor
  const automations = await getAutomationsForExecutor(connection, executor.publicKey);
  
  if (automations.length === 0) {
    return { deployed: 0, skipped: 0, errors: 0 };
  }
  
  // Batch fetch all miner accounts for eligibility check
  const authorities = automations.map(a => a.data.authority);
  const minerDataMap = await batchGetMinerAccounts(connection, authorities);
  
  // Filter to automations that can deploy
  const eligibleAutomations = [];
  for (const automation of automations) {
    const minerData = minerDataMap.get(automation.data.authority.toBase58()) || null;
    if (canDeployWithMinerData(automation.data, minerData, boardData.roundId)) {
      eligibleAutomations.push(automation);
    } else {
      skipped++;
    }
  }
  
  if (eligibleAutomations.length === 0) {
    return { deployed, skipped, errors };
  }
  
  console.log(`[${new Date().toISOString()}] Round ${boardData.roundId}: ${eligibleAutomations.length} automations ready to deploy`);
  
  // Wait for buffer period to end before deploying anyone
  const canProceed = await waitForBufferEnd(connection, boardPda);
  if (!canProceed) {
    console.log(`  Round ended while waiting, skipping...`);
    return { deployed: 0, skipped: eligibleAutomations.length, errors: 0 };
  }
  
  // Process in batches
  for (let i = 0; i < eligibleAutomations.length; i += BATCH_SIZE) {
    const batch = eligibleAutomations.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(eligibleAutomations.length / BATCH_SIZE);
    
    if (totalBatches > 1) {
      console.log(`  Processing batch ${batchNum}/${totalBatches} (${batch.length} accounts)...`);
    }
    
    const result = await processBatch(
      program,
      connection,
      executor,
      batch,
      boardPda,
      configPda,
      feeCollector,
      boardData.roundId
    );
    
    deployed += result.deployed;
    errors += result.errors;
    
    // If we got buffer errors, wait and retry the whole batch once
    if (result.bufferErrors > 0) {
      console.log(`  ${result.bufferErrors} buffer errors, waiting 3s and retrying...`);
      await sleep(3000);
      
      const retryResult = await processBatch(
        program,
        connection,
        executor,
        batch,
        boardPda,
        configPda,
        feeCollector,
        boardData.roundId
      );
      
      deployed += retryResult.deployed;
      errors += retryResult.errors + retryResult.bufferErrors;
    }
    
    // Small delay between batches to avoid rate limiting
    if (i + BATCH_SIZE < eligibleAutomations.length) {
      await sleep(500);
    }
  }
  
  console.log(`  Summary: ${deployed} deployed, ${skipped} skipped, ${errors} errors`);
  
  return { deployed, skipped, errors };
}

async function main() {
  const connection = new Connection(SOLANA_RPC_URL!, {
    commitment: "confirmed",
    confirmTransactionInitialTimeout: 60000,
  });
  const executor = loadKeypair(PRIVATE_KEY!);
  
  const wallet = new anchor.Wallet(executor);
  const provider = new anchor.AnchorProvider(connection, wallet, {
    preflightCommitment: "confirmed",
  });

  console.log("============================================================");
  console.log("DARK MATTER - Auto-Deploy Cranker (Batch Processing)");
  console.log("============================================================");
  console.log(`Program ID: ${PROGRAM_ID.toBase58()}`);
  console.log(`Executor: ${executor.publicKey.toBase58()}`);
  console.log(`RPC: ${SOLANA_RPC_URL?.includes('helius') ? 'Helius' : SOLANA_RPC_URL?.slice(0, 50)}...`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Priority fee: ${PRIORITY_FEE} microlamports`);
  console.log("============================================================\n");

  // Derive PDAs
  const boardPda = deriveBoardPda();
  const configPda = deriveConfigPda();
  
  // Get fee collector from config
  const configInfo = await connection.getAccountInfo(configPda);
  if (!configInfo) {
    console.error("ERROR: Config not found. Is the program initialized?");
    process.exit(1);
  }
  const configData = decodeConfig(configInfo.data as Buffer);
  const feeCollector = configData.feeCollector;
  console.log(`Fee Collector: ${feeCollector.toBase58()}`);

  // Load program IDL
  const idl = await anchor.Program.fetchIdl(PROGRAM_ID, provider);
  if (!idl) {
    console.error("ERROR: Could not fetch IDL. Is the program deployed?");
    process.exit(1);
  }
  const program = new anchor.Program(idl, provider);

  let totalDeployed = 0;
  let totalErrors = 0;

  console.log("Starting auto-deploy cranker loop...\n");

  // Check for automations using this executor
  const automations = await getAutomationsForExecutor(connection, executor.publicKey);
  console.log(`Found ${automations.length} automation(s) using this executor\n`);

  while (true) {
    try {
      const { deployed, errors } = await runAutoDeployCycle(
        program,
        connection,
        executor,
        boardPda,
        configPda,
        feeCollector
      );
      totalDeployed += deployed;
      totalErrors += errors;
    } catch (err: any) {
      console.error(`Error in auto-deploy cycle: ${err.message}`);
    }

    await sleep(POLL_INTERVAL_MS);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
