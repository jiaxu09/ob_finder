const { Client, Databases, ID, Query } = require("node-appwrite");
const axios = require("axios");
const nodemailer = require("nodemailer");

// ============================================================================
// --- ENHANCED ORDER BLOCK DETECTION WITH VOLUME & BALANCE FILTER ---
//
// ✅ 核心功能：
// 1. SMA20 成交量确认（Volume > SMA20 × 1.2）
// 2. 平衡度计算与过滤（仅保留 20% - 80% 之间）
// 3. 多维度风险评估与通知
// 4. 交易时段识别与可靠性标记（新增）
// ============================================================================

// ============================================================================
// --- 辅助函数 ---
// ============================================================================

async function sendTelegramNotification(config, message, context) {
  if (
    !config.ENABLE_TELEGRAM ||
    !config.TELEGRAM_BOT_TOKEN ||
    !config.TELEGRAM_CHAT_ID
  )
    return;
  const url = `https://api.telegram.org/bot${config.TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await axios.post(url, {
      chat_id: config.TELEGRAM_CHAT_ID,
      text: message,
      parse_mode: "Markdown",
    });
    context.log("✅ Telegram notification sent successfully.");
  } catch (e) {
    context.error(
      "❌ Failed to send Telegram notification:",
      e.response ? e.response.data : e.message
    );
  }
}

async function sendEmailNotification(config, subject, body, context) {
  if (
    !config.ENABLE_EMAIL ||
    !config.EMAIL_RECIPIENT ||
    !config.EMAIL_CONFIG.auth.user ||
    !config.EMAIL_CONFIG.auth.pass
  )
    return;
  let transporter = nodemailer.createTransport(config.EMAIL_CONFIG);
  try {
    let info = await transporter.sendMail({
      from: `Crypto Alerter <${config.EMAIL_CONFIG.auth.user}>`,
      to: config.EMAIL_RECIPIENT,
      subject: subject,
      text: body,
      html: `<pre>${body}</pre>`,
    });
    context.log(`✅ Email notification sent. Message ID: ${info.messageId}`);
  } catch (e) {
    context.error("❌ Failed to send Email notification:", e);
  }
}

async function getKlines(symbol, interval, limit, context) {
  const url = `https://api.binance.com/api/v3/klines`;
  try {
    const response = await axios.get(url, {
      params: { symbol, interval, limit },
    });
    return response.data.map((k) => ({
      timestamp: new Date(k[0]),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
    }));
  } catch (e) {
    context.error(`Failed to get klines for ${symbol} ${interval}:`, e.message);
    return null;
  }
}

// ============================================================================
// --- 🆕 交易时段识别函数 ---
// ============================================================================

/**
 * 判断给定时间是否为周末
 * @param {Date} date - 要检查的时间
 * @returns {boolean} 是否为周末
 */
function isWeekend(date) {
  const day = date.getUTCDay();
  return day === 0 || day === 6; // 0=Sunday, 6=Saturday
}

/**
 * 获取给定时间的交易时段
 * @param {Date} date - 要检查的时间
 * @returns {Object} { session: string, emoji: string, reliable: boolean, description: string }
 */
function getMarketSession(date) {
  const hour = date.getUTCHours();
  
  // 检查是否为周末
  if (isWeekend(date)) {
    return {
      session: "周末",
      emoji: "⛔",
      reliable: false,
      description: "周末低流动性时段"
    };
  }
  
  // 判断交易时段（UTC 时间）
  const sessions = [];
  
  // 亚洲时段: 00:00-09:00 UTC (东京 09:00-18:00 JST, 香港 08:00-17:00 HKT)
  if (hour >= 0 && hour < 9) {
    sessions.push("亚洲");
  }
  
  // 欧洲时段: 07:00-16:00 UTC (伦敦 08:00-17:00 BST/GMT)
  if (hour >= 7 && hour < 16) {
    sessions.push("欧洲");
  }
  
  // 美股时段: 13:30-20:00 UTC (纽约 09:30-16:00 EST/EDT)
  if ((hour === 13 && date.getUTCMinutes() >= 30) || (hour >= 14 && hour < 20)) {
    sessions.push("美股");
  }
  
  // 如果没有匹配任何主要时段，标记为低流动性
  if (sessions.length === 0) {
    return {
      session: "非交易时段",
      emoji: "⚠️",
      reliable: false,
      description: "低流动性时段"
    };
  }
  
  // 如果有重叠时段（高流动性），显示所有相关时段
  const sessionName = sessions.join(" + ");
  const emoji = sessions.length > 1 ? "🔥" : "✅";
  
  return {
    session: sessionName,
    emoji: emoji,
    reliable: true,
    description: sessions.length > 1 ? "多市场重叠 - 高流动性" : "单一市场时段"
  };
}

// ============================================================================
// --- Order Block 技术指标计算 ---
// ============================================================================

/**
 * 计算单根 K 线的真实波幅 (True Range)
 */
function calculateTrueRange(kline, prevKline) {
  const high = kline.high;
  const low = kline.low;
  const prevClose = prevKline ? prevKline.close : kline.close;
  
  return Math.max(
    high - low,
    Math.abs(high - prevClose),
    Math.abs(low - prevClose)
  );
}

/**
 * 计算 ATR，使用 RMA/EMA 方法
 */
function calculateAtrEma(klines, period = 10) {
  if (klines.length < period) return 0;
  
  const trs = klines.map((k, i) => calculateTrueRange(k, i > 0 ? klines[i - 1] : null));
  
  const alpha = 1 / period; 
  let atr = trs.slice(1, period + 1).reduce((sum, val) => sum + val, 0) / period;
  
  for (let i = period + 1; i < trs.length; i++) {
    atr = (trs[i] * alpha) + (atr * (1 - alpha));
  }
  
  return atr;
}

/**
 * 计算成交量的简单移动平均线 (SMA)
 */
function calculateVolumeSMA(klines, endIndex, period = 20) {
  if (endIndex < period - 1) return 0;
  
  let sum = 0;
  for (let i = endIndex - period + 1; i <= endIndex; i++) {
    if (i >= 0 && i < klines.length) {
      sum += klines[i].volume;
    }
  }
  return sum / period;
}

/**
 * ✅ [新增] 计算平衡度
 * @param {number} obHighVolume 高成交量部分
 * @param {number} obLowVolume 低成交量部分
 * @returns {number} 平衡度百分比 (0-100)
 */
function calculateBalancePercentage(obHighVolume, obLowVolume) {
  const maxVol = Math.max(obHighVolume, obLowVolume);
  const minVol = Math.min(obHighVolume, obLowVolume);
  
  if (maxVol === 0) return 0;
  
  return Math.round((minVol / maxVol) * 100);
}

/**
 * ✅ [新增] 评估平衡度质量
 * @param {number} balance 平衡度百分比
 * @returns {string} 质量评级
 */
function evaluateBalanceQuality(balance) {
  if (balance >= 60 && balance <= 80) return "🟢 优秀";
  if (balance >= 40 && balance < 60) return "🟡 良好";
  if (balance >= 20 && balance < 40) return "🟠 一般";
  return "🔴 较差";
}

/**
 * ✅ [增强版] Order Block 识别 - 带成交量确认与平衡度过滤
 */
function findOrderBlocksPineScriptLogic(
  klines,
  swingLength = 10,
  obEndMethod = "Wick",
  maxATRMult = 3.5,
  volumeMultiplier = 1.2,
  volumeSMAPeriod = 20,
  minBalancePercent = 20,  // ✅ 新增参数
  maxBalancePercent = 80   // ✅ 新增参数
) {
  const bullishOBs = [];
  const bearishOBs = [];
  
  // 📊 统计信息
  const stats = {
    totalBullishSignals: 0,
    totalBearishSignals: 0,
    bullishRejectedByVolume: 0,
    bearishRejectedByVolume: 0,
    bullishRejectedByBalance: 0,  // ✅ 新增
    bearishRejectedByBalance: 0,  // ✅ 新增
  };
  
  let swingType = 0;
  let lastSwingHigh = null;
  let lastSwingLow = null;
  
  const atr = calculateAtrEma(klines, 10);
  
  for (let barIndex = swingLength; barIndex < klines.length; barIndex++) {
    const refIndex = barIndex - swingLength;
    
    let upper = -Infinity;
    let lower = Infinity;
    
    for (let j = refIndex + 1; j <= barIndex; j++) {
      if (j < klines.length) {
        upper = Math.max(upper, klines[j].high);
        lower = Math.min(lower, klines[j].low);
      }
    }
    
    // Swing High 识别
    if (klines[refIndex].high > upper) {
      if (swingType !== 0) {
        lastSwingHigh = { index: refIndex, high: klines[refIndex].high, crossed: false };
      }
      swingType = 0;
    }
    
    // Swing Low 识别
    if (klines[refIndex].low < lower) {
      if (swingType !== 1) {
        lastSwingLow = { index: refIndex, low: klines[refIndex].low, crossed: false };
      }
      swingType = 1;
    }
    
    const currentCandle = klines[barIndex];
    
    // ============ 🟢 看涨 OB 形成（带成交量确认 + 平衡度过滤）============
    if (lastSwingHigh && !lastSwingHigh.crossed && currentCandle.close > lastSwingHigh.high) {
      lastSwingHigh.crossed = true;
      stats.totalBullishSignals++;
      
      // ✅ 第一步：成交量确认
      const volumeSMA20 = calculateVolumeSMA(klines, barIndex, volumeSMAPeriod);
      const volumeThreshold = volumeSMA20 * volumeMultiplier;
      
      if (currentCandle.volume <= volumeThreshold) {
        stats.bullishRejectedByVolume++;
        continue; // ❌ 成交量不足，忽略此OB
      }
      
      let boxBtm = barIndex >= 1 ? klines[barIndex - 1].high : currentCandle.high;
      let boxTop = barIndex >= 1 ? klines[barIndex - 1].low : currentCandle.low;
      let boxLoc = barIndex >= 1 ? klines[barIndex - 1].timestamp : currentCandle.timestamp;
      
      const distance = barIndex - lastSwingHigh.index;
      for (let i = 1; i <= distance - 1; i++) {
        const candleIndex = barIndex - i;
        const minVal = klines[candleIndex].low;
        const maxVal = klines[candleIndex].high;
        
        if (minVal < boxBtm) {
          boxBtm = minVal;
          boxTop = maxVal;
          boxLoc = klines[candleIndex].timestamp;
        }
      }
      
      const vol0 = currentCandle.volume;
      const vol1 = barIndex >= 1 ? klines[barIndex - 1].volume : 0;
      const vol2 = barIndex >= 2 ? klines[barIndex - 2].volume : 0;
      const obVolume = vol0 + vol1 + vol2;
      const obLowVolume = vol2;
      const obHighVolume = vol0 + vol1;
      
      // ✅ 第二步：平衡度过滤
      const balancePercent = calculateBalancePercentage(obHighVolume, obLowVolume);
      
      if (balancePercent < minBalancePercent || balancePercent > maxBalancePercent) {
        stats.bullishRejectedByBalance++;
        continue; // ❌ 平衡度不符合要求，忽略此OB
      }
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        bullishOBs.unshift({
          startTime: boxLoc,
          confirmationTime: currentCandle.timestamp,
          top: boxTop,
          bottom: boxBtm,
          obVolume,
          obLowVolume,
          obHighVolume,
          breakoutVolume: currentCandle.volume,
          volumeSMA20,
          volumeRatio: (currentCandle.volume / volumeSMA20).toFixed(2),
          balancePercent,  // ✅ 新增：平衡度
          balanceQuality: evaluateBalanceQuality(balancePercent),  // ✅ 新增：平衡度评级
          isValid: true,
          breaker: false,
          breakTime: null,
          type: "Support"
        });
      }
    }
    
    // ============ 🔴 看跌 OB 形成（带成交量确认 + 平衡度过滤）============
    if (lastSwingLow && !lastSwingLow.crossed && currentCandle.close < lastSwingLow.low) {
      lastSwingLow.crossed = true;
      stats.totalBearishSignals++;
      
      // ✅ 第一步：成交量确认
      const volumeSMA20 = calculateVolumeSMA(klines, barIndex, volumeSMAPeriod);
      const volumeThreshold = volumeSMA20 * volumeMultiplier;
      
      if (currentCandle.volume <= volumeThreshold) {
        stats.bearishRejectedByVolume++;
        continue; // ❌ 成交量不足，忽略此OB
      }
      
      let boxBtm = barIndex >= 1 ? klines[barIndex - 1].low : currentCandle.low;
      let boxTop = barIndex >= 1 ? klines[barIndex - 1].high : currentCandle.high;
      let boxLoc = barIndex >= 1 ? klines[barIndex - 1].timestamp : currentCandle.timestamp;
      
      const distance = barIndex - lastSwingLow.index;
      for (let i = 1; i <= distance - 1; i++) {
        const candleIndex = barIndex - i;
        const maxVal = klines[candleIndex].high;
        const minVal = klines[candleIndex].low;
        
        if (maxVal > boxTop) {
          boxTop = maxVal;
          boxBtm = minVal;
          boxLoc = klines[candleIndex].timestamp;
        }
      }
      
      const vol0 = currentCandle.volume;
      const vol1 = barIndex >= 1 ? klines[barIndex - 1].volume : 0;
      const vol2 = barIndex >= 2 ? klines[barIndex - 2].volume : 0;
      const obVolume = vol0 + vol1 + vol2;
      const obLowVolume = vol0 + vol1;
      const obHighVolume = vol2;
      
      // ✅ 第二步：平衡度过滤
      const balancePercent = calculateBalancePercentage(obHighVolume, obLowVolume);
      
      if (balancePercent < minBalancePercent || balancePercent > maxBalancePercent) {
        stats.bearishRejectedByBalance++;
        continue; // ❌ 平衡度不符合要求，忽略此OB
      }
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        bearishOBs.unshift({
          startTime: boxLoc,
          confirmationTime: currentCandle.timestamp,
          top: boxTop,
          bottom: boxBtm,
          obVolume,
          obLowVolume,
          obHighVolume,
          breakoutVolume: currentCandle.volume,
          volumeSMA20,
          volumeRatio: (currentCandle.volume / volumeSMA20).toFixed(2),
          balancePercent,  // ✅ 新增：平衡度
          balanceQuality: evaluateBalanceQuality(balancePercent),  // ✅ 新增：平衡度评级
          isValid: true,
          breaker: false,
          breakTime: null,
          type: "Resistance"
        });
      }
    }
    
    // ============ OB 失效检测 ============
    for (let ob of bullishOBs) {
      if (!ob.breaker) {
        const testValue = obEndMethod === "Wick" ? currentCandle.low : Math.min(currentCandle.open, currentCandle.close);
        if (testValue < ob.bottom) {
          ob.breaker = true;
          ob.breakTime = currentCandle.timestamp;
        }
      } else {
        if (currentCandle.high > ob.top) ob.isValid = false;
      }
    }
    
    for (let ob of bearishOBs) {
      if (!ob.breaker) {
        const testValue = obEndMethod === "Wick" ? currentCandle.high : Math.max(currentCandle.open, currentCandle.close);
        if (testValue > ob.top) {
          ob.breaker = true;
          ob.breakTime = currentCandle.timestamp;
        }
      } else {
        if (currentCandle.low < ob.bottom) ob.isValid = false;
      }
    }
  }
  
  return {
    bullishOBs: bullishOBs.filter(ob => ob.isValid),
    bearishOBs: bearishOBs.filter(ob => ob.isValid),
    stats // ✅ 返回过滤统计信息
  };
}

// ============================================================================
// --- Appwrite Function Entrypoint ---
// ============================================================================
module.exports = async (context) => {
  context.log("🚀 Function execution started...");

  const CONFIG = {
    SYMBOLS: ["BTCUSDT", "ETHUSDT"],
    TIMEZONES: ["1h", "4h", "1d"],
    SWING_LENGTH: 10,
    OB_END_METHOD: "Wick",
    MAX_ATR_MULT: 3.5,
    KLINE_LIMIT: 1000,
    
    // ✅ 成交量过滤参数
    VOLUME_MULTIPLIER: 1.2,
    VOLUME_SMA_PERIOD: 20,
    
    // ✅ 新增：平衡度过滤参数
    MIN_BALANCE_PERCENT: 20,  // 最小平衡度 20%
    MAX_BALANCE_PERCENT: 80,  // 最大平衡度 80%

    ENABLE_TELEGRAM: true,
    TELEGRAM_BOT_TOKEN: "7607543807:AAFcNXDZE_ctPhTQVc60vnX69o0zPjzsLb0",
    TELEGRAM_CHAT_ID: "7510264240",

    ENABLE_EMAIL: true,
    EMAIL_RECIPIENT: "jiaxu99.w@gmail.com",
    EMAIL_CONFIG: {
      service: "gmail",
      auth: { user: "jiaxu99.w@gmail.com", pass: "hqmv qwbm qpik juiq" },
    },
  };

  const client = new Client()
    .setEndpoint('https://syd.cloud.appwrite.io/v1')
    .setProject('68f59e58002322d3d474')
    .setKey('standard_2555e90b24b6442cafa174ecccc387d2668557a61d73186f705f7e65681f9ed2cbbf5a672f55669cb9a549a5a8a282b2f1dd32e3f3a1a818dd06c2ce4e23f72da594fddd5dfcd736f0bb04d1151962a6fb9568a25c700e8d4746eddc96ec2538556dd23e696117ad6ebdbdb05856a5250fb125e03b3484fd6b73e24d245c59e8');

  const databases = new Databases(client);
  const DB_ID = "68f5a3fa001774a5ab3d";
  const COLLECTION_ID = "seen_zones";

  async function loadPreviousZones() {
    try {
      const response = await databases.listDocuments(DB_ID, COLLECTION_ID, [ Query.limit(5000) ]);
      return new Set(response.documents.map((doc) => doc.zoneIdentifier));
    } catch (e) {
      context.error("Failed to load previous zones from Appwrite DB:", e);
      return new Set();
    }
  }

  async function saveNewZone(zoneIdentifier) {
    try {
      await databases.createDocument(DB_ID, COLLECTION_ID, ID.unique(), { zoneIdentifier });
      return true;
    } catch (e) {
      if (e.code !== 409) context.error(`Failed to save new zone ID "${zoneIdentifier}":`, e);
      return false;
    }
  }

  async function analyzeSymbol(symbol, context) {
    context.log(`\n📊 Analyzing ${symbol}...`);
    const previousZones = await loadPreviousZones();
    const newNotifications = [];

    for (const tf of CONFIG.TIMEZONES) {
      const klines = await getKlines(symbol, tf, CONFIG.KLINE_LIMIT, context);
      if (!klines || klines.length <= CONFIG.SWING_LENGTH) {
        context.log(`⚠️ Insufficient data for ${symbol} ${tf}, skipping.`);
        continue;
      }

      const { bullishOBs, bearishOBs, stats } = findOrderBlocksPineScriptLogic(
        klines,
        CONFIG.SWING_LENGTH,
        CONFIG.OB_END_METHOD,
        CONFIG.MAX_ATR_MULT,
        CONFIG.VOLUME_MULTIPLIER,
        CONFIG.VOLUME_SMA_PERIOD,
        CONFIG.MIN_BALANCE_PERCENT,  // ✅ 传入平衡度参数
        CONFIG.MAX_BALANCE_PERCENT   // ✅ 传入平衡度参数
      );
      
      // ✅ 增强日志：显示完整过滤统计
      context.log(
        `${symbol} ${tf}: ` +
        `🟢 ${bullishOBs.length} bullish OBs ` +
        `(${stats.bullishRejectedByVolume} by volume, ${stats.bullishRejectedByBalance} by balance) | ` +
        `🔴 ${bearishOBs.length} bearish OBs ` +
        `(${stats.bearishRejectedByVolume} by volume, ${stats.bearishRejectedByBalance} by balance)`
      );
      
      const allZones = [...bullishOBs, ...bearishOBs];

      for (const zone of allZones.slice(0, 5)) {
        const zoneIdentifier = `${symbol}-${tf}-${zone.startTime.getTime()}-${zone.type}`;
        
        if (!previousZones.has(zoneIdentifier)) {
          context.log(`🆕 New zone detected: ${zoneIdentifier} (Balance: ${zone.balancePercent}%)`);
          const saved = await saveNewZone(zoneIdentifier);
          
          if (saved) {
            const formatNZTime = (date) => date.toLocaleString("en-NZ", {
              timeZone: "Pacific/Auckland",
              year: "numeric",
              month: "2-digit",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
              hour12: false,
            });

            const status = zone.breaker 
              ? `🟡 已触及 (Breaker) @ ${formatNZTime(zone.breakTime)}`
              : `🟢 有效`;

            // ✅ 🆕 获取交易时段信息
            const sessionInfo = getMarketSession(zone.confirmationTime);
            const reliabilityWarning = !sessionInfo.reliable 
              ? `\n⚠️ *注意: ${sessionInfo.description}，信号可靠性较低*` 
              : '';

            // ✅ 增强通知消息：包含平衡度信息 + 交易时段信息
            const message = `*🔔 新 Order Block 区域警报*\n\n` +
              `*交易对:* ${symbol}\n` +
              `*时间周期:* ${tf}\n` +
              `*类型:* ${zone.type === "Support" ? "🟢 看涨支撑区" : "🔴 看跌阻力区"}\n` +
              `*状态:* ${status}\n` +
              `*价格区间:* ${zone.bottom.toFixed(zone.bottom > 100 ? 2 : 4)} - ${zone.top.toFixed(zone.top > 100 ? 2 : 4)}\n\n` +
              `*📊 成交量确认 (已通过)*\n` +
              `• 突破K线成交量: ${zone.breakoutVolume.toFixed(0)}\n` +
              `• SMA20基准: ${zone.volumeSMA20.toFixed(0)}\n` +
              `• 成交量比率: ${zone.volumeRatio}x (>1.2✅)\n\n` +
              `*⚖️ 平衡度分析*\n` +
              `• 平衡度: ${zone.balancePercent}% ${zone.balanceQuality}\n` +
              `• 有效范围: 20%-80% ✅\n` +
              `• 总成交量: ${zone.obVolume.toFixed(0)}\n` +
              `• 高量部分: ${zone.obHighVolume.toFixed(0)}\n` +
              `• 低量部分: ${zone.obLowVolume.toFixed(0)}\n\n` +
              `*⏰ 时间与时段信息*\n` +
              `• OB 形成时间: ${formatNZTime(zone.startTime)}\n` +
              `• 突破确认时间: ${formatNZTime(zone.confirmationTime)}\n` +
              `• 确认时段: ${sessionInfo.emoji} *${sessionInfo.session}*\n` +
              `• 时段描述: ${sessionInfo.description}${reliabilityWarning}\n\n` +
              `_此区域已通过成交量与平衡度双重验证_`;

            newNotifications.push({
              message,
              subject: `🔔 ${symbol} ${tf} 新 ${zone.type} 区域 [平衡度: ${zone.balancePercent}%] [${sessionInfo.session}]`,
            });
          }
        }
      }
    }
    return newNotifications;
  }

  const allNewNotifications = [];
  
  for (const symbol of CONFIG.SYMBOLS) {
    const notifications = await analyzeSymbol(symbol, context);
    allNewNotifications.push(...notifications);
  }

  if (allNewNotifications.length > 0) {
    context.log(`\n✉️ Sending ${allNewNotifications.length} notification(s)...`);
    for (const n of allNewNotifications) {
      await sendTelegramNotification(CONFIG, n.message, context);
      await sendEmailNotification(CONFIG, n.subject, n.message, context);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  } else {
    context.log("\n✅ No new zones found across all symbols.");
  }

  context.log("\n🎉 Function execution finished successfully.");
  return context.res.json({
    success: true,
    new_zones_found: allNewNotifications.length,
    timestamp: new Date().toISOString()
  });
};