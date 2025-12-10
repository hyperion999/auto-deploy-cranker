/**
 * Auto-Deploy Cranker for Dark Matter
 * 
 * Railway-compatible service that automatically deploys for users with automation enabled.
 * Executes auto_deploy for all active automation accounts.
 * 
 * Environment Variables:
 *   SOLANA_RPC_URL    - Solana RPC endpoint (required)
 *   PRIVATE_KEY       - Executor wallet private key as JSON array (required)
 *   PROGRAM_ID        - Dark Matter program ID (required)
 *   DRY_RUN           - Set to "1" for dry run mode (optional)
 *   POLL_INTERVAL_MS  - Polling interval in ms (default: 5000)
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
  LAMPORTS_PER_SOL
} from "@solana/web3.js";
import * as anchor from "@coral-xyz/anchor";

// Load environment variables
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const PROGRAM_ID_STR = process.env.PROGRAM_ID;
const DRY_RUN = process.env.DRY_RUN === "1";
const POLL_INTERVAL_MS = parseInt(process.env.POLL_INTERVAL_MS || "5000");

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

async function canDeploy(
  connection: Connection,
  automation: ReturnType<typeof decodeAutomation>,
  currentRoundId: number
): Promise<boolean> {
  // Check if automation has sufficient balance
  const minRequired = automation.amountPerTile + automation.executorFee + CHECKPOINT_FEE;
  if (automation.balance < minRequired) {
    return false;
  }
  
  // Check if miner needs checkpoint first
  const minerPda = deriveMinerPda(automation.authority);
  const minerInfo = await connection.getAccountInfo(minerPda);
  
  if (minerInfo) {
    const minerData = decodeMiner(minerInfo.data as Buffer);
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

async function autoDeploy(
  program: anchor.Program,
  connection: Connection,
  executor: Keypair,
  automation: { pubkey: PublicKey; data: ReturnType<typeof decodeAutomation> },
  boardPda: PublicKey,
  configPda: PublicKey,
  feeCollector: PublicKey,
  currentRoundId: number
): Promise<boolean> {
  const minerPda = deriveMinerPda(automation.data.authority);
  const roundPda = deriveRoundPda(currentRoundId);
  
  if (DRY_RUN) {
    console.log(`  [DRY RUN] Would auto-deploy for ${automation.data.authority.toBase58().slice(0, 8)}...`);
    console.log(`    Balance: ${automation.data.balance / LAMPORTS_PER_SOL} SOL`);
    console.log(`    Amount/tile: ${automation.data.amountPerTile / LAMPORTS_PER_SOL} SOL`);
    console.log(`    Strategy: ${automation.data.strategy === 0 ? 'Random' : 'Preferred'}`);
    return true;
  }
  
  try {
    await program.methods
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
      .rpc();
    
    console.log(`  ✓ Auto-deployed for ${automation.data.authority.toBase58().slice(0, 8)}...`);
    return true;
  } catch (err: any) {
    console.error(`  ✗ Failed for ${automation.data.authority.toBase58().slice(0, 8)}...: ${err.message}`);
    return false;
  }
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
    // Still in buffer period
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
  
  // Filter to automations that can deploy
  const eligibleAutomations = [];
  for (const automation of automations) {
    if (await canDeploy(connection, automation.data, boardData.roundId)) {
      eligibleAutomations.push(automation);
    } else {
      skipped++;
    }
  }
  
  if (eligibleAutomations.length === 0) {
    return { deployed, skipped, errors };
  }
  
  console.log(`[${new Date().toISOString()}] Round ${boardData.roundId}: ${eligibleAutomations.length} automations ready to deploy`);
  
  for (const automation of eligibleAutomations) {
    const success = await autoDeploy(
      program,
      connection,
      executor,
      automation,
      boardPda,
      configPda,
      feeCollector,
      boardData.roundId
    );
    
    if (success) {
      deployed++;
    } else {
      errors++;
    }
    
    await sleep(200); // Rate limiting
  }
  
  if (deployed > 0) {
    console.log(`  Summary: ${deployed} deployed, ${skipped} skipped, ${errors} errors`);
  }
  
  return { deployed, skipped, errors };
}

async function main() {
  const connection = new Connection(SOLANA_RPC_URL!, "confirmed");
  const executor = loadKeypair(PRIVATE_KEY!);
  
  const wallet = new anchor.Wallet(executor);
  const provider = new anchor.AnchorProvider(connection, wallet, {
    preflightCommitment: "confirmed",
  });

  console.log("============================================================");
  console.log("DARK MATTER - Auto-Deploy Cranker");
  console.log("============================================================");
  console.log(`Program ID: ${PROGRAM_ID.toBase58()}`);
  console.log(`Executor: ${executor.publicKey.toBase58()}`);
  console.log(`RPC: ${SOLANA_RPC_URL}`);
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
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

