const { Client, Databases, ID, Query } = require("node-appwrite");
const axios = require("axios");
const nodemailer = require("nodemailer");

// ============================================================================
// --- PINE SCRIPT LOGIC ALIGNMENT SUMMARY ---
//
// 1.  ✅ Order Block (OB) Identification: Core logic for swing points,
//      confirmation, candle search, and zone definition is a 1:1 match.
// 2.  ✅ ATR Calculation: Uses an EMA-based method, matching ta.atr().
// 3.  ✅ OB Invalidation: Two-stage breaker/invalidation logic is a 1:1 match.
//      This version correctly identifies and reports "breaker" zones.
// 4.  ⚠️ Not Implemented: Multi-timeframe analysis (request.security) and
//      OB zone combination (combineOBsFunc) are complex features specific
//      to the Pine Script environment and are not included here.
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
// --- Order Block 技术指标计算 ---
// ============================================================================

/**
 * 计算单根 K 线的真实波幅 (True Range)
 * @param {object} kline 当前 K 线
 * @param {object} prevKline 前一根 K 线
 * @returns {number} 真实波幅
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
 * ✅ [关键更新] 计算 ATR，使用 RMA/EMA 方法，与 Pine Script 的 ta.atr() 一致
 * @param {Array<object>} klines K 线数据
 * @param {number} period 周期，默认为 10
 * @returns {number} 当前的 ATR 值
 */
function calculateAtrEma(klines, period = 10) {
  if (klines.length < period) return 0;
  
  const trs = klines.map((k, i) => calculateTrueRange(k, i > 0 ? klines[i - 1] : null));
  
  const alpha = 1 / period; 
  
  // 初始化 ATR：计算前 N 个 TR 的简单平均值
  let atr = trs.slice(1, period + 1).reduce((sum, val) => sum + val, 0) / period;
  
  // 使用 RMA/EMA 公式进行平滑计算
  for (let i = period + 1; i < trs.length; i++) {
    atr = (trs[i] * alpha) + (atr * (1 - alpha));
  }
  
  return atr;
}

/**
 * ✅ 完全按照 Pine Script 逻辑实现的 OB 识别
 * @param {Array<object>} klines K 线数据
 * @param {number} swingLength 摆动点长度
 * @param {string} obEndMethod 失效方式 "Wick" 或 "Close"
 * @param {number} maxATRMult ATR 乘数
 * @returns {{bullishOBs: Array<object>, bearishOBs: Array<object>}}
 */
function findOrderBlocksPineScriptLogic(
  klines,
  swingLength = 10,
  obEndMethod = "Wick",
  maxATRMult = 3.5
) {
  const bullishOBs = [];
  const bearishOBs = [];
  
  let swingType = 0;
  let lastSwingHigh = null;
  let lastSwingLow = null;
  
  const atr = calculateAtrEma(klines, 10);
  
  for (let barIndex = swingLength; barIndex < klines.length; barIndex++) {
    const refIndex = barIndex - swingLength;
    
    // 计算 ta.highest(len) 和 ta.lowest(len)
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
    
    // ============ 看涨 OB 形成 ============
    if (lastSwingHigh && !lastSwingHigh.crossed && currentCandle.close > lastSwingHigh.high) {
      lastSwingHigh.crossed = true;
      
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
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        bullishOBs.unshift({
          startTime: boxLoc, confirmationTime: currentCandle.timestamp, top: boxTop,
          bottom: boxBtm, obVolume, obLowVolume, obHighVolume, isValid: true,
          breaker: false, breakTime: null, type: "Support"
        });
      }
    }
    
    // ============ 看跌 OB 形成 ============
    if (lastSwingLow && !lastSwingLow.crossed && currentCandle.close < lastSwingLow.low) {
      lastSwingLow.crossed = true;
      
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
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        bearishOBs.unshift({
          startTime: boxLoc, confirmationTime: currentCandle.timestamp, top: boxTop,
          bottom: boxBtm, obVolume, obLowVolume, obHighVolume, isValid: true,
          breaker: false, breakTime: null, type: "Resistance"
        });
      }
    }
    
    // ============ OB 失效检测 (对所有历史 OB 进行) ============
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
    bearishOBs: bearishOBs.filter(ob => ob.isValid)
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
    OB_END_METHOD: "Wick", // "Wick" 或 "Close"
    MAX_ATR_MULT: 3.5,
    KLINE_LIMIT: 1000,

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

      const { bullishOBs, bearishOBs } = findOrderBlocksPineScriptLogic(
        klines, CONFIG.SWING_LENGTH, CONFIG.OB_END_METHOD, CONFIG.MAX_ATR_MULT
      );
      
      context.log(`${symbol} ${tf}: Found ${bullishOBs.length} 🟢 bullish | ${bearishOBs.length} 🔴 bearish OBs`);
      
      const allZones = [...bullishOBs, ...bearishOBs];

      for (const zone of allZones.slice(0, 5)) { // 只检查最新的5个OB
        const zoneIdentifier = `${symbol}-${tf}-${zone.startTime.getTime()}-${zone.type}`;
        
        if (!previousZones.has(zoneIdentifier)) {
          context.log(`🆕 New zone detected: ${zoneIdentifier}`);
          const saved = await saveNewZone(zoneIdentifier);
          
          if (saved) {
            const formatNZTime = (date) => date.toLocaleString("en-NZ", {
              timeZone: "Pacific/Auckland", year: "numeric", month: "2-digit", day: "2-digit",
              hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
            });

            const percentage = Math.round(
              (Math.min(zone.obHighVolume, zone.obLowVolume) / Math.max(zone.obHighVolume, zone.obLowVolume) || 0) * 100
            );

            const status = zone.breaker 
              ? `🟡 已触及 (Breaker) @ ${formatNZTime(zone.breakTime)}`
              : `🟢 有效`;

            const message = `*🔔 新 Order Block 区域警报*\n\n` +
              `*交易对:* ${symbol}\n` +
              `*时间周期:* ${tf}\n` +
              `*类型:* ${zone.type === "Support" ? "🟢 看涨支撑区" : "🔴 看跌阻力区"}\n` +
              `*状态:* ${status}\n` +
              `*价格区间:* ${zone.bottom.toFixed(zone.bottom > 100 ? 2 : 4)} - ${zone.top.toFixed(zone.top > 100 ? 2 : 4)}\n` +
              `*总成交量:* ${zone.obVolume.toFixed(0)} (平衡度: ${percentage}%)\n` +
              `*OB 形成时间:* ${formatNZTime(zone.startTime)}\n` +
              `*突破确认时间:* ${formatNZTime(zone.confirmationTime)}\n\n` +
              `_此区域基于 Pine Script 逻辑识别_`;

            newNotifications.push({
              message,
              subject: `🔔 ${symbol} ${tf} 新 ${zone.type} 区域`,
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