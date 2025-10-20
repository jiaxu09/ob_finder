const { Client, Databases, ID, Query } = require('node-appwrite');
const axios = require('axios');
const nodemailer = require('nodemailer');

// ============================================================================
// --- 辅助函数 ---
// 这些函数支持主逻辑的运行
// ============================================================================

/**
 * 发送 Telegram 消息
 * @param {object} config - 包含 Telegram 配置的对象
 * @param {string} message - 要发送的消息
 * @param {function} log - Appwrite 的日志函数
 * @param {function} error - Appwrite 的错误日志函数
 */
async function sendTelegramNotification(config, message, log, error) {
    if (!config.ENABLE_TELEGRAM || !config.TELEGRAM_BOT_TOKEN || !config.TELEGRAM_CHAT_ID) {
        return;
    }
    const url = `https://api.telegram.org/bot${config.TELEGRAM_BOT_TOKEN}/sendMessage`;
    try {
        await axios.post(url, {
            chat_id: config.TELEGRAM_CHAT_ID,
            text: message,
            parse_mode: 'Markdown'
        });
        log('✅ Telegram notification sent successfully.');
    } catch (e) {
        error('❌ Failed to send Telegram notification:', e.response ? e.response.data : e.message);
    }
}

/**
 * 发送邮件通知
 * @param {object} config - 包含 Email 配置的对象
 * @param {string} subject - 邮件主题
 * @param {string} body - 邮件正文
 * @param {function} log - Appwrite 的日志函数
 * @param {function} error - Appwrite 的错误日志函数
 */
async function sendEmailNotification(config, subject, body, log, error) {
    if (!config.ENABLE_EMAIL || !config.EMAIL_RECIPIENT || !config.EMAIL_CONFIG.auth.user || !config.EMAIL_CONFIG.auth.pass) {
        return;
    }
    let transporter = nodemailer.createTransport(config.EMAIL_CONFIG);

    try {
        let info = await transporter.sendMail({
            from: `Crypto Alerter <${config.EMAIL_CONFIG.auth.user}>`,
            to: config.EMAIL_RECIPIENT,
            subject: subject,
            text: body,
            html: `<pre>${body}</pre>`
        });
        log(`✅ Email notification sent. Message ID: ${info.messageId}`);
    } catch (e) {
        error('❌ Failed to send Email notification:', e);
    }
}

/**
 * 从币安获取K线数据
 * @param {string} symbol - 交易对
 * @param {string} interval - 时间周期
 * @param {number} limit - K线数量
 * @param {function} error - Appwrite 的错误日志函数
 * @returns {Promise<Array|null>}
 */
async function getKlines(symbol, interval, limit, error) {
    const url = `https://api.binance.com/api/v3/klines`;
    try {
        const response = await axios.get(url, { params: { symbol, interval, limit } });
        return response.data.map(k => ({
            timestamp: new Date(k[0]),
            open: parseFloat(k[1]),
            high: parseFloat(k[2]),
            low: parseFloat(k[3]),
            close: parseFloat(k[4]),
            volume: parseFloat(k[5])
        }));
    } catch (e) {
        error(`Failed to get klines for ${symbol} ${interval}:`, e.message);
        return null;
    }
}

/**
 * 核心分析逻辑：模拟Pine Script的状态机寻找订单块
 * @param {Array} klines - K线数据
 * @param {number} length - 摆动点长度
 * @returns {{bullishOBs: Array, bearishOBs: Array}}
 */
function findOrderBlocksStatefulSimulation(klines, length) {
    const bullishOBs = [], bearishOBs = [];
    let lastSwingHigh = null, lastSwingLow = null;

    for (let i = length; i < klines.length; i++) {
        const refIndex = i - length;
        const windowSlice = klines.slice(refIndex + 1, i + 1);
        if (windowSlice.length === 0) continue;

        const maxHighInWindow = Math.max(...windowSlice.map(c => c.high));
        if (klines[refIndex].high > maxHighInWindow) {
            lastSwingHigh = { ...klines[refIndex], index: refIndex, crossed: false };
        }

        const minLowInWindow = Math.min(...windowSlice.map(c => c.low));
        if (klines[refIndex].low < minLowInWindow) {
            lastSwingLow = { ...klines[refIndex], index: refIndex, crossed: false };
        }

        const currentCandle = klines[i];
        if (lastSwingHigh && !lastSwingHigh.crossed && currentCandle.close > lastSwingHigh.high) {
            lastSwingHigh.crossed = true;
            const searchRange = klines.slice(lastSwingHigh.index, i);
            if (searchRange.length > 0) {
                const obCandle = searchRange.reduce((prev, curr) => (prev.low < curr.low ? prev : curr));
                bullishOBs.push({ startTime: obCandle.timestamp, top: obCandle.high, bottom: obCandle.low, isValid: true });
            }
        }

        if (lastSwingLow && !lastSwingLow.crossed && currentCandle.close < lastSwingLow.low) {
            lastSwingLow.crossed = true;
            const searchRange = klines.slice(lastSwingLow.index, i);
            if (searchRange.length > 0) {
                const obCandle = searchRange.reduce((prev, curr) => (prev.high > curr.high ? prev : curr));
                bearishOBs.push({ startTime: obCandle.timestamp, top: obCandle.high, bottom: obCandle.low, isValid: true });
            }
        }

        for (const ob of bullishOBs) if (ob.isValid && currentCandle.low < ob.bottom) ob.isValid = false;
        for (const ob of bearishOBs) if (ob.isValid && currentCandle.high > ob.top) ob.isValid = false;
    }
    return { bullishOBs, bearishOBs };
}


// ============================================================================
// --- Appwrite Function Entrypoint ---
// 这是函数执行的入口点
// ============================================================================
module.exports = async ({ req, res, log, error }) => {
    log('Function execution started...');

    // --- 1. 初始化配置和 Appwrite Client ---
    // 从环境变量中获取配置
    const CONFIG = {
        SYMBOL: process.env.SYMBOL || 'BTCUSDT',
        TIMEZONES: (process.env.TIMEZONES || '1h,4h,1d').split(','),
        SWING_LENGTH: parseInt(process.env.SWING_LENGTH || '10'),
        KLINE_LIMIT: 1000,
        
        ENABLE_TELEGRAM: process.env.ENABLE_TELEGRAM === 'true',
        TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID: process.env.TELEGRAM_CHAT_ID,

        ENABLE_EMAIL: process.env.ENABLE_EMAIL === 'true',
        EMAIL_RECIPIENT: process.env.EMAIL_RECIPIENT,
        EMAIL_CONFIG: {
            service: 'gmail',
            auth: {
                user: process.env.GMAIL_USER,
                pass: process.env.GMAIL_APP_PASSWORD
            }
        }
    };

    const client = new Client()
        .setEndpoint(process.env.APPWRITE_FUNCTION_ENDPOINT)
        .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
        .setKey(process.env.APPWRITE_API_KEY);

    const databases = new Databases(client);
    const DB_ID = process.env.APPWRITE_DATABASE_ID;
    const COLLECTION_ID = process.env.APPWRITE_COLLECTION_ID;

    // --- 2. 状态管理 (使用 Appwrite Database) ---
    async function loadPreviousZones() {
        try {
            // Appwrite 建议分页查询，但对于几千条记录，一次性获取也问题不大
            const response = await databases.listDocuments(DB_ID, COLLECTION_ID, [Query.limit(5000)]);
            const identifiers = response.documents.map(doc => doc.zoneIdentifier);
            return new Set(identifiers);
        } catch (e) {
            error('Failed to load previous zones from Appwrite DB:', e);
            return new Set();
        }
    }

    async function saveNewZone(zoneIdentifier) {
        try {
            await databases.createDocument(DB_ID, COLLECTION_ID, ID.unique(), { zoneIdentifier });
        } catch (e) {
            // 避免因重复创建（极小概率并发）而报错
            if (e.code !== 409) {
                 error(`Failed to save new zone identifier "${zoneIdentifier}" to Appwrite DB:`, e);
            }
        }
    }
    
    // --- 3. 主逻辑 ---
    log(`Starting analysis for ${CONFIG.SYMBOL}...`);
    const previousZones = await loadPreviousZones();
    const newNotifications = [];

    for (const tf of CONFIG.TIMEZONES) {
        const klines = await getKlines(CONFIG.SYMBOL, tf, CONFIG.KLINE_LIMIT, error);
        if (!klines || klines.length <= CONFIG.SWING_LENGTH) {
            log(`Insufficient data for ${tf}, skipping.`);
            continue;
        }

        const { bullishOBs, bearishOBs } = findOrderBlocksStatefulSimulation(klines, CONFIG.SWING_LENGTH);
        const allZones = [
            ...bullishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Support' })),
            ...bearishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Resistance' }))
        ];

        for (const zone of allZones) {
            if (typeof zone.bottom !== 'number' || typeof zone.top !== 'number') {
                log('Skipping malformed zone:', zone);
                continue;
            }

            // 使用时间戳和类型作为区域的唯一标识符
            const zoneIdentifier = `${zone.startTime.getTime()}-${zone.type}`;
            if (!previousZones.has(zoneIdentifier)) {
                log(`New zone found: ${zoneIdentifier}`);
                const message = `*🔔 新区域警报: ${CONFIG.SYMBOL} (${tf})*\n\n*类型:* ${zone.type === 'Support' ? '🟢 支撑区 (Bullish OB)' : '🔴 阻力区 (Bearish OB)'}\n*价格范围:* ${zone.bottom.toFixed(4)} - ${zone.top.toFixed(4)}\n*形成时间:* ${zone.startTime.toLocaleString()}`;
                newNotifications.push({ message, subject: `新 ${tf} ${zone.type} 区域: ${CONFIG.SYMBOL}` });
                // 立即保存到数据库，防止重复通知
                await saveNewZone(zoneIdentifier);
            }
        }
    }

    if (newNotifications.length > 0) {
        log(`Found ${newNotifications.length} new zones. Sending notifications...`);
        // 使用 Promise.all 并行发送通知，提高效率
        await Promise.all(newNotifications.map(notification => {
            return Promise.all([
                sendTelegramNotification(CONFIG, notification.message, log, error),
                sendEmailNotification(CONFIG, notification.subject, notification.message, log, error)
            ]);
        }));
    } else {
        log('No new zones found.');
    }

    log('Function execution finished successfully.');
    return res.json({ success: true, new_zones_found: newNotifications.length });
};