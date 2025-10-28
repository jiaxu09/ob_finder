const { Client, Databases, ID, Query } = require('node-appwrite');
const axios = require('axios');
const nodemailer = require('nodemailer');

// ============================================================================
// --- 辅助函数 (已适配 Appwrite context) ---
// ============================================================================

async function sendTelegramNotification(config, message, context) {
    if (!config.ENABLE_TELEGRAM || !config.TELEGRAM_BOT_TOKEN || !config.TELEGRAM_CHAT_ID) return;
    const url = `https://api.telegram.org/bot${config.TELEGRAM_BOT_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: config.TELEGRAM_CHAT_ID, text: message, parse_mode: 'Markdown' });
        context.log('✅ Telegram notification sent successfully.');
    } catch (e) {
        context.error('❌ Failed to send Telegram notification:', e.response ? e.response.data : e.message);
    }
}

async function sendEmailNotification(config, subject, body, context) {
    if (!config.ENABLE_EMAIL || !config.EMAIL_RECIPIENT || !config.EMAIL_CONFIG.auth.user || !config.EMAIL_CONFIG.auth.pass) return;
    let transporter = nodemailer.createTransport(config.EMAIL_CONFIG);
    try {
        let info = await transporter.sendMail({
            from: `Crypto Alerter <${config.EMAIL_CONFIG.auth.user}>`, to: config.EMAIL_RECIPIENT, subject: subject, text: body, html: `<pre>${body}</pre>`
        });
        context.log(`✅ Email notification sent. Message ID: ${info.messageId}`);
    } catch (e) {
        context.error('❌ Failed to send Email notification:', e);
    }
}

async function getKlines(symbol, interval, limit, context) {
    const url = `https://api.binance.com/api/v3/klines`;
    try {
        const response = await axios.get(url, { params: { symbol, interval, limit } });
        return response.data.map(k => ({ timestamp: new Date(k[0]), open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]), volume: parseFloat(k[5]) }));
    } catch (e) {
        context.error(`Failed to get klines for ${symbol} ${interval}:`, e.message);
        return null;
    }
}

function findOrderBlocksStatefulSimulation(klines, swingLength, volumeLookback = 20, volumeThresholdPercentile = 70) {
    const bullishOBs = [];
    const bearishOBs = [];
    let lastSwingHigh = null;
    let lastSwingLow = null;

    // 预计算成交量分位数（用于动态阈值）
    function getVolumeThreshold(startIndex, endIndex) {
        const vols = klines.slice(startIndex, endIndex).map(k => k.volume).filter(v => v > 0);
        if (vols.length === 0) return 0;
        vols.sort((a, b) => a - b);
        const idx = Math.floor((volumeThresholdPercentile / 100) * (vols.length - 1));
        return vols[idx];
    }

    for (let i = swingLength; i < klines.length; i++) {
        const refIndex = i - swingLength;
        const windowSlice = klines.slice(refIndex + 1, i + 1);
        if (windowSlice.length === 0) continue;

        // === 识别 Swing High ===
        const maxHighInWindow = Math.max(...windowSlice.map(c => c.high));
        if (klines[refIndex].high > maxHighInWindow) {
            lastSwingHigh = { ...klines[refIndex], index: refIndex, crossed: false };
        }

        // === 识别 Swing Low ===
        const minLowInWindow = Math.min(...windowSlice.map(c => c.low));
        if (klines[refIndex].low < minLowInWindow) {
            lastSwingLow = { ...klines[refIndex], index: refIndex, crossed: false };
        }

        const currentCandle = klines[i];

        // === 多头 Order Block：价格上破 Swing High ===
        if (lastSwingHigh && !lastSwingHigh.crossed && currentCandle.close > lastSwingHigh.high) {
            // ✅ 规则2：突破K线需放量
            const volThresholdForBreakout = getVolumeThreshold(Math.max(0, i - volumeLookback), i);
            if (currentCandle.volume >= volThresholdForBreakout) {
                lastSwingHigh.crossed = true;
                const searchRange = klines.slice(lastSwingHigh.index, i);
                if (searchRange.length > 0) {
                    // ✅ 规则1：在区间内找“最低低点 + 高成交量”的K线作为 OB
                    let bestCandle = null;
                    const volThresholdForOB = getVolumeThreshold(Math.max(0, lastSwingHigh.index - volumeLookback), i);
                    for (const candle of searchRange) {
                        if (candle.volume >= volThresholdForOB) {
                            if (!bestCandle || candle.low < bestCandle.low) {
                                bestCandle = candle;
                            }
                        }
                    }
                    // 如果找不到高量K线，退而求其次用最低点（但标记为低置信）
                    if (!bestCandle) {
                        bestCandle = searchRange.reduce((prev, curr) => (prev.low < curr.low ? prev : curr));
                    }
                    bullishOBs.push({
                        startTime: bestCandle.timestamp,
                        top: bestCandle.high,
                        bottom: bestCandle.low,
                        volume: bestCandle.volume,
                        isValid: true,
                        confidence: bestCandle.volume >= volThresholdForOB ? 'high' : 'low'
                    });
                }
            }
        }

        // === 空头 Order Block：价格下破 Swing Low ===
        if (lastSwingLow && !lastSwingLow.crossed && currentCandle.close < lastSwingLow.low) {
            const volThresholdForBreakout = getVolumeThreshold(Math.max(0, i - volumeLookback), i);
            if (currentCandle.volume >= volThresholdForBreakout) {
                lastSwingLow.crossed = true;
                const searchRange = klines.slice(lastSwingLow.index, i);
                if (searchRange.length > 0) {
                    let bestCandle = null;
                    const volThresholdForOB = getVolumeThreshold(Math.max(0, lastSwingLow.index - volumeLookback), i);
                    for (const candle of searchRange) {
                        if (candle.volume >= volThresholdForOB) {
                            if (!bestCandle || candle.high > bestCandle.high) {
                                bestCandle = candle;
                            }
                        }
                    }
                    if (!bestCandle) {
                        bestCandle = searchRange.reduce((prev, curr) => (prev.high > curr.high ? prev : curr));
                    }
                    bearishOBs.push({
                        startTime: bestCandle.timestamp,
                        top: bestCandle.high,
                        bottom: bestCandle.low,
                        volume: bestCandle.volume,
                        isValid: true,
                        confidence: bestCandle.volume >= volThresholdForOB ? 'high' : 'low'
                    });
                }
            }
        }

        // === 动态失效：价格穿过 OB 区域 ===
        for (const ob of bullishOBs) {
            if (ob.isValid && currentCandle.low < ob.bottom) ob.isValid = false;
        }
        for (const ob of bearishOBs) {
            if (ob.isValid && currentCandle.high > ob.top) ob.isValid = false;
        }
    }

    return { bullishOBs, bearishOBs };
}

// ============================================================================
// --- Appwrite Function Entrypoint ---
// ============================================================================
module.exports = async (context) => {
    context.log('Function execution started...');

    // --- 1. 从环境变量加载配置 (安全方式) ---
    const CONFIG = {
        SYMBOL:  'BTCUSDT',
        TIMEZONES: ('1h,4h,1d').split(','),
        SWING_LENGTH: parseInt( '10'),
        KLINE_LIMIT: 1000,
        
        ENABLE_TELEGRAM: 'true',
        TELEGRAM_BOT_TOKEN: '7607543807:AAFcNXDZE_ctPhTQVc60vnX69o0zPjzsLb0',
        TELEGRAM_CHAT_ID: '7510264240',

        ENABLE_EMAIL:  'true',
        EMAIL_RECIPIENT: 'jiaxu99.w@gmail.com',
        EMAIL_CONFIG: {
            service: 'gmail',
            auth: {
                user: 'jiaxu99.w@gmail.com',
                pass: 'hqmv qwbm qpik juiq'
            }
        }
    };

    // --- 2. 初始化 Appwrite Client ---
    const client = new Client()
        .setEndpoint('https://syd.cloud.appwrite.io/v1')
        .setProject('68f59e58002322d3d474')
        .setKey('standard_2555e90b24b6442cafa174ecccc387d2668557a61d73186f705f7e65681f9ed2cbbf5a672f55669cb9a549a5a8a282b2f1dd32e3f3a1a818dd06c2ce4e23f72da594fddd5dfcd736f0bb04d1151962a6fb9568a25c700e8d4746eddc96ec2538556dd23e696117ad6ebdbdb05856a5250fb125e03b3484fd6b73e24d245c59e8');

    const databases = new Databases(client);
    const DB_ID = '68f5a3fa001774a5ab3d';
    const COLLECTION_ID = 'seen_zones';

    // --- 3. 状态管理 (使用 Appwrite Database) ---
    async function loadPreviousZones() {
        try {
            const response = await databases.listDocuments(DB_ID, COLLECTION_ID, [Query.limit(5000)]);
            return new Set(response.documents.map(doc => doc.zoneIdentifier));
        } catch (e) {
            context.error('Failed to load previous zones from Appwrite DB:', e);
            return new Set();
        }
    }

    async function saveNewZone(zoneIdentifier) {
        try {
            await databases.createDocument(DB_ID, COLLECTION_ID, ID.unique(), { zoneIdentifier });
        } catch (e) {
            if (e.code !== 409) context.error(`Failed to save new zone ID "${zoneIdentifier}":`, e);
        }
    }

    // --- 4. 主逻辑 ---
    context.log(`Starting analysis for ${CONFIG.SYMBOL}...`);
    const previousZones = await loadPreviousZones();
    const newNotifications = [];

    for (const tf of CONFIG.TIMEZONES) {
        const klines = await getKlines(CONFIG.SYMBOL, tf, CONFIG.KLINE_LIMIT, context);
        if (!klines || klines.length <= CONFIG.SWING_LENGTH) {
            context.log(`Insufficient data for ${tf}, skipping.`);
            continue;
        }

        const { bullishOBs, bearishOBs } = findOrderBlocksStatefulSimulation(klines, CONFIG.SWING_LENGTH,20,70);
        const allZones = [
            ...bullishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Support' })),
            ...bearishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Resistance' }))
        ];

        for (const zone of allZones) {
            if (typeof zone.bottom !== 'number' || typeof zone.top !== 'number') continue;
            const zoneIdentifier = `${zone.startTime.getTime()}-${zone.type}`;
            if (!previousZones.has(zoneIdentifier)) {
                context.log(`New zone found: ${zoneIdentifier}`);
                const message = `*🔔 新区域警报: ${CONFIG.SYMBOL} (${tf})*\n\n*类型:* ${zone.type === 'Support' ? '🟢 支撑区' : '🔴 阻力区'}\n*价格范围:* ${zone.bottom.toFixed(4)} - ${zone.top.toFixed(4)}\n*形成时间:* ${zone.startTime.toLocaleString()}`;
                newNotifications.push({ message, subject: `新 ${tf} ${zone.type} 区域: ${CONFIG.SYMBOL}` });
                await saveNewZone(zoneIdentifier);
            }
        }
    }

    if (newNotifications.length > 0) {
        context.log(`Found ${newNotifications.length} new zones. Sending notifications...`);
        await Promise.all(newNotifications.map(n => Promise.all([
            sendTelegramNotification(CONFIG, n.message, context),
            sendEmailNotification(CONFIG, n.subject, n.message, context)
        ])));
    } else {
        context.log('No new zones found.');
    }

    context.log('Function execution finished successfully.');
    return context.res.json({ success: true, new_zones_found: newNotifications.length });
};