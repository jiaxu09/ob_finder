const axios = require('axios');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');

// ============================================================================
// --- ⚙️ 用户配置 ---
// ============================================================================
const CONFIG = {
    // 交易对和时间周期
    SYMBOL: 'BTCUSDT',
    TIMEZONES: ['1h', '4h', '1d'],

    // 订单块分析参数
    SWING_LENGTH: 10,
    KLINE_LIMIT: 1000,

    // --- Telegram 通知配置 ---
    ENABLE_TELEGRAM: true, // 设置为 true 来启用 Telegram 通知
    TELEGRAM_BOT_TOKEN: '7607543807:AAFcNXDZE_ctPhTQVc60vnX69o0zPjzsLb0', // 替换为你的 Bot Token
    TELEGRAM_CHAT_ID: '7510264240',   // 替换为你的 Chat ID

    // --- Email 通知配置 ---
    ENABLE_EMAIL: true, // 设置为 true 来启用 Email 通知
    EMAIL_RECIPIENT: 'jiaxu99.w@gmail.com', // 接收通知的邮箱地址

    // 邮件服务商配置 (SMTP)
    // 选项1: 使用 Ethereal 进行免费测试 (https://ethereal.email/)
    EMAIL_CONFIG_ETHEREAL: {
        host: 'smtp.ethereal.email',
        port: 587,
        auth: {
            user: 'ethereal-user@ethereal.email', // 替换为你的 Ethereal 用户名
            pass: 'ethereal-password'           // 替换为你的 Ethereal 密码
        }
    },
    // 选项2: 使用 Gmail (需要应用密码)
    EMAIL_CONFIG_GMAIL: {
        service: 'gmail',
        auth: {
            user: 'jiaxu99.w@gmail.com', // 你的 Gmail 邮箱
            pass: 'hqmv qwbm qpik juiq'      // 你的 16 位 Google 应用密码
        }
    },
    
    // 选择要使用的邮件配置
    ACTIVE_EMAIL_CONFIG: 'EMAIL_CONFIG_GMAIL' // 改为 'EMAIL_CONFIG_ETHEREAL' 来使用 Ethereal
};

const STATE_FILE_PATH = path.join(__dirname, 'previous_zones.json');

// ============================================================================
// --- 通知模块 ---
// ============================================================================

/**
 * 发送 Telegram 消息
 * @param {string} message 要发送的消息
 */
async function sendTelegramNotification(message) {
    if (!CONFIG.ENABLE_TELEGRAM || CONFIG.TELEGRAM_BOT_TOKEN === '在此处粘贴你的机器人TOKEN') {
        return;
    }
    const url = `https://api.telegram.org/bot${CONFIG.TELEGRAM_BOT_TOKEN}/sendMessage`;
    try {
        await axios.post(url, {
            chat_id: CONFIG.TELEGRAM_CHAT_ID,
            text: message,
            parse_mode: 'Markdown'
        });
        console.log('✅ Telegram 通知已发送');
    } catch (error) {
        console.error('❌ 发送 Telegram 通知失败:', error.response ? error.response.data : error.message);
    }
}

/**
 * 发送邮件
 * @param {string} subject 邮件主题
 * @param {string} body 邮件正文
 */
async function sendEmailNotification(subject, body) {
    if (!CONFIG.ENABLE_EMAIL || CONFIG.EMAIL_RECIPIENT === 'recipient@example.com') {
        return;
    }
    const emailConfig = CONFIG[CONFIG.ACTIVE_EMAIL_CONFIG];
    let transporter = nodemailer.createTransport(emailConfig);

    try {
        let info = await transporter.sendMail({
            from: `Crypto Alerter <${emailConfig.auth.user}>`,
            to: CONFIG.EMAIL_RECIPIENT,
            subject: subject,
            text: body,
            html: `<pre>${body}</pre>`
        });
        console.log('✅ Email 通知已发送. Message ID:', info.messageId);
        if (CONFIG.ACTIVE_EMAIL_CONFIG === 'EMAIL_CONFIG_ETHEREAL') {
             console.log('📬 Ethereal 预览 URL:', nodemailer.getTestMessageUrl(info));
        }
    } catch (error) {
        console.error('❌ 发送 Email 通知失败:', error);
    }
}

// ============================================================================
// --- 状态管理模块 ---
// ============================================================================

/**
 * 加载之前已通知的区域
 * @returns {Set<number>} 一个包含区域时间戳的集合
 */
function loadPreviousZones() {
    try {
        if (fs.existsSync(STATE_FILE_PATH)) {
            const data = fs.readFileSync(STATE_FILE_PATH, 'utf8');
            return new Set(JSON.parse(data));
        }
    } catch (error) {
        console.error('读取状态文件失败:', error);
    }
    return new Set();
}

/**
 * 保存当前所有区域以备下次比较
 * @param {Set<number>} zoneTimestamps 包含所有当前区域时间戳的集合
 */
function saveCurrentZones(zoneTimestamps) {
    try {
        fs.writeFileSync(STATE_FILE_PATH, JSON.stringify(Array.from(zoneTimestamps)));
    } catch (error) {
        console.error('保存状态文件失败:', error);
    }
}

// ============================================================================
// --- 核心分析逻辑 ---
// ============================================================================

async function getKlines(symbol, interval, limit = 1000) {
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
    } catch (error) {
        console.error(`获取 ${symbol} ${interval} 数据时出错:`, error.message);
        return null;
    }
}

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
// --- 主函数 (已集成错误修复) ---
// ============================================================================
async function main() {
    console.log(`[${new Date().toLocaleString()}] 开始检查新的支撑/阻力区域...`);

    const previousZoneTimestamps = loadPreviousZones();
    const allCurrentZoneTimestamps = new Set();
    const newNotifications = [];

    for (const tf of CONFIG.TIMEZONES) {
        console.log(`--- 正在分析 ${CONFIG.SYMBOL} 在 ${tf} 时间周期 ---`);
        const klines = await getKlines(CONFIG.SYMBOL, tf, CONFIG.KLINE_LIMIT);
        if (!klines || klines.length <= CONFIG.SWING_LENGTH) {
            console.log(`数据不足，跳过 ${tf} 分析。`);
            continue;
        }

        const { bullishOBs, bearishOBs } = findOrderBlocksStatefulSimulation(klines, CONFIG.SWING_LENGTH);
        const allZones = [
            ...bullishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Support' })),
            ...bearishOBs.filter(ob => ob.isValid).map(z => ({ ...z, type: 'Resistance' }))
        ];

        for (const zone of allZones) {
            
            // --- 错误修复：健壮性检查 ---
            // 检查 zone.bottom 和 zone.top 是否存在且为数字
            if (typeof zone.bottom !== 'number' || typeof zone.top !== 'number') {
                console.warn('⚠️ 发现一个格式不正确的区域，已跳过:', zone);
                continue; // 跳过这个格式不正确的区域，处理下一个
            }
            // --- 检查结束 ---

            const zoneTimestamp = zone.startTime.getTime();
            allCurrentZoneTimestamps.add(zoneTimestamp);

            // 如果这个区域是新的，则创建通知
            if (!previousZoneTimestamps.has(zoneTimestamp)) {
                const message = 
`*🔔 新区域警报: ${CONFIG.SYMBOL} (${tf})*

*类型:* ${zone.type === 'Support' ? '🟢 支撑区 (Bullish OB)' : '🔴 阻力区 (Bearish OB)'}
*价格范围:* ${zone.bottom.toFixed(4)} - ${zone.top.toFixed(4)}
*形成时间:* ${zone.startTime.toLocaleString()}`;
                
                newNotifications.push({ 
                    message, 
                    subject: `新 ${tf} ${zone.type} 区域: ${CONFIG.SYMBOL}` 
                });
            }
        }
    }
    
    // 发送所有新通知
    if (newNotifications.length > 0) {
        console.log(`发现 ${newNotifications.length} 个新区域，正在发送通知...`);
        for (const notification of newNotifications) {
            await sendTelegramNotification(notification.message);
            await sendEmailNotification(notification.subject, notification.message);
        }
    } else {
        console.log('未发现新区域。');
    }

    // 更新状态文件
    saveCurrentZones(allCurrentZoneTimestamps);
    console.log('检查完成。\n');
}

main();