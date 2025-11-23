const { Client, Databases, Storage, ID, Query, InputFile } = require("node-appwrite");
const axios = require("axios");
const nodemailer = require("nodemailer");

// ============================================================================
// --- 配置区域 ---
// ============================================================================
const RUNTIME_CONFIG = {
  EXECUTION_INTERVAL_MINUTES: 5,
  
  TIMEFRAME_WINDOWS: {
    '1m': 15,
    '5m': 20,
    '15m': 30,
    '1h': 90,
    '4h': 300,
    '1d': 1500,
  },
  
  STORAGE_CONFIG: {
    BUCKET_ID: "69212fae00370f2eaf74",
    FILE_ID: "seen_zones.json",
    CLEANUP_DAYS: 30,
    AUTO_CREATE_BUCKET: false,
  },
  
  DB_CONFIG: {
    SYNC_HOUR_UTC: 2,
    SYNC_WINDOW_MINUTES: 10,
    DAYS_LOOKBACK: 30,
    SAVE_FULL_DATA: true,
    USE_DB_FALLBACK: true,
  },
};

// ============================================================================
// --- 突破K线形态分析模块 ---
// ============================================================================

function analyzeBreakoutCandlePattern(breakoutCandle, obType) {
  const { open, high, low, close } = breakoutCandle;
  
  const totalRange = high - low;
  const body = Math.abs(close - open);
  const bodyPercent = totalRange > 0 ? (body / totalRange) * 100 : 0;
  
  const isBullish = close > open;
  
  const upperWick = isBullish ? high - close : high - open;
  const lowerWick = isBullish ? open - low : close - low;
  const upperWickPercent = totalRange > 0 ? (upperWick / totalRange) * 100 : 0;
  const lowerWickPercent = totalRange > 0 ? (lowerWick / totalRange) * 100 : 0;
  
  const priceChangePercent = open > 0 ? ((close - open) / open) * 100 : 0;
  
  let candleType = "";
  let candleEmoji = "";
  let strengthScore = 0;
  
  if (bodyPercent >= 70) {
    candleType = isBullish ? "强势阳线 (Marubozu)" : "强势阴线 (Marubozu)";
    candleEmoji = isBullish ? "🟢💪" : "🔴💪";
    strengthScore = 90;
  } else if (bodyPercent >= 50) {
    candleType = isBullish ? "标准阳线" : "标准阴线";
    candleEmoji = isBullish ? "🟩" : "🟥";
    strengthScore = 70;
  } else if (bodyPercent >= 30) {
    candleType = isBullish ? "小阳线" : "小阴线";
    candleEmoji = isBullish ? "⬆️" : "⬇️";
    strengthScore = 50;
  } else if (bodyPercent <= 10) {
    if (upperWickPercent > 40 && lowerWickPercent < 20) {
      candleType = "流星线/上吊线 (Shooting Star)";
      candleEmoji = "☄️";
      strengthScore = 30;
    } else if (lowerWickPercent > 40 && upperWickPercent < 20) {
      candleType = "锤子线 (Hammer)";
      candleEmoji = "🔨";
      strengthScore = isBullish ? 60 : 35;
    } else {
      candleType = "十字星 (Doji)";
      candleEmoji = "✝️";
      strengthScore = 20;
    }
  } else {
    if (isBullish && lowerWickPercent > 30 && upperWickPercent < 15) {
      candleType = "锤子线";
      candleEmoji = "🔨";
      strengthScore = 65;
    } else if (!isBullish && upperWickPercent > 30 && lowerWickPercent < 15) {
      candleType = "流星线";
      candleEmoji = "☄️";
      strengthScore = 35;
    } else {
      candleType = isBullish ? "普通阳线" : "普通阴线";
      candleEmoji = isBullish ? "📈" : "📉";
      strengthScore = 45;
    }
  }
  
  let finalScore = strengthScore;
  
  const isDirectionMatched = 
    (obType === "Support" && isBullish) ||
    (obType === "Resistance" && !isBullish);
  
  if (!isDirectionMatched) {
    finalScore -= 30;
  }
  
  if (bodyPercent >= 60 && Math.max(upperWickPercent, lowerWickPercent) < 20) {
    finalScore += 10;
  }
  if (bodyPercent < 20 || Math.max(upperWickPercent, lowerWickPercent) > 50) {
    finalScore -= 15;
  }
  
  finalScore = Math.min(100, Math.max(0, finalScore));
  
  let breakoutStrength = "";
  let breakoutEmoji = "";
  let recommendation = "";
  
  if (finalScore >= 80) {
    breakoutStrength = "极强";
    breakoutEmoji = "🔥🔥🔥";
    recommendation = "高置信度信号，可重点关注";
  } else if (finalScore >= 60) {
    breakoutStrength = "强";
    breakoutEmoji = "🔥🔥";
    recommendation = "较强信号，建议关注";
  } else if (finalScore >= 40) {
    breakoutStrength = "中等";
    breakoutEmoji = "🔥";
    recommendation = "中性信号，谨慎对待";
  } else if (finalScore >= 25) {
    breakoutStrength = "偏弱";
    breakoutEmoji = "⚠️";
    recommendation = "信号偏弱，建议等待确认";
  } else {
    breakoutStrength = "弱";
    breakoutEmoji = "❌";
    recommendation = "弱信号，不建议跟进";
  }
  
  return {
    isBullish,
    direction: isBullish ? "看涨" : "看跌",
    totalRange: totalRange.toFixed(8),
    body: body.toFixed(8),
    bodyPercent: bodyPercent.toFixed(1),
    upperWick: upperWick.toFixed(8),
    lowerWick: lowerWick.toFixed(8),
    upperWickPercent: upperWickPercent.toFixed(1),
    lowerWickPercent: lowerWickPercent.toFixed(1),
    priceChangePercent: priceChangePercent.toFixed(2),
    candleType,
    candleEmoji,
    strengthScore: finalScore,
    breakoutStrength,
    breakoutEmoji,
    isDirectionMatched,
    directionMatchEmoji: isDirectionMatched ? "✅" : "⚠️",
    recommendation,
    description: generateCandleDescription(bodyPercent, upperWickPercent, lowerWickPercent, isBullish)
  };
}

function generateCandleDescription(bodyPercent, upperWickPercent, lowerWickPercent, isBullish) {
  const direction = isBullish ? "上涨" : "下跌";
  
  let bodyDesc = "";
  if (bodyPercent >= 70) bodyDesc = "超大实体";
  else if (bodyPercent >= 50) bodyDesc = "大实体";
  else if (bodyPercent >= 30) bodyDesc = "中等实体";
  else if (bodyPercent >= 15) bodyDesc = "小实体";
  else bodyDesc = "极小实体";
  
  let wickDesc = "";
  const maxWick = Math.max(upperWickPercent, lowerWickPercent);
  const wickDiff = Math.abs(upperWickPercent - lowerWickPercent);
  
  if (maxWick < 10) {
    wickDesc = "几乎无影线，果断";
  } else if (wickDiff < 15) {
    wickDesc = "上下影线均衡";
  } else if (upperWickPercent > lowerWickPercent * 2) {
    wickDesc = isBullish ? "上影线较长，上方压力明显" : "上影线较长，卖压较重";
  } else if (lowerWickPercent > upperWickPercent * 2) {
    wickDesc = isBullish ? "下影线较长，下方支撑较强" : "下影线较长，有买盘承接";
  } else {
    wickDesc = "影线比例正常";
  }
  
  return `${direction}${bodyDesc}，${wickDesc}`;
}

// ============================================================================
// --- 辅助函数 ---
// ============================================================================

async function sendTelegramNotification(config, message, context) {
  if (!config.ENABLE_TELEGRAM || !config.TELEGRAM_BOT_TOKEN || !config.TELEGRAM_CHAT_ID) return;
  const url = `https://api.telegram.org/bot${config.TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await axios.post(url, {
      chat_id: config.TELEGRAM_CHAT_ID,
      text: message,
      parse_mode: "Markdown",
    });
    context.log("✅ Telegram notification sent successfully.");
  } catch (e) {
    context.error("❌ Failed to send Telegram notification:", e.response ? e.response.data : e.message);
  }
}

async function sendEmailNotification(config, subject, body, context) {
  if (!config.ENABLE_EMAIL || !config.EMAIL_RECIPIENT || !config.EMAIL_CONFIG.auth.user || !config.EMAIL_CONFIG.auth.pass) return;
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

function isWeekend(date) {
  const day = date.getUTCDay();
  return day === 0 || day === 6;
}

function getMarketSession(date) {
  const hour = date.getUTCHours();
  
  if (isWeekend(date)) {
    return {
      session: "周末",
      emoji: "⛔",
      reliable: false,
      description: "周末低流动性时段"
    };
  }
  
  const sessions = [];
  
  if (hour >= 0 && hour < 9) {
    sessions.push("亚洲");
  }
  
  if (hour >= 7 && hour < 16) {
    sessions.push("欧洲");
  }
  
  if ((hour === 13 && date.getUTCMinutes() >= 30) || (hour >= 14 && hour < 20)) {
    sessions.push("美股");
  }
  
  if (sessions.length === 0) {
    return {
      session: "非交易时段",
      emoji: "⚠️",
      reliable: false,
      description: "低流动性时段"
    };
  }
  
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
// --- 🆕 完整的OB详细信息显示函数 ---
// ============================================================================

function formatOBDetails(ob, index, symbol, timeframe) {
  const bp = ob.breakoutPattern;
  const sessionInfo = getMarketSession(ob.confirmationTime);
  
  const formatTime = (date) => date.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
  const priceDecimal = ob.top > 100 ? 2 : 6;
  
  return `
╔═══════════════════════════════════════════════════════════════════════════════
║ OB #${index + 1} - ${symbol} ${timeframe} - ${ob.type === "Support" ? "🟢 BULLISH SUPPORT" : "🔴 BEARISH RESISTANCE"}
╠═══════════════════════════════════════════════════════════════════════════════
║ 📍 价格区间
║   ├─ Top:    ${ob.top.toFixed(priceDecimal)}
║   ├─ Bottom: ${ob.bottom.toFixed(priceDecimal)}
║   └─ Range:  ${((ob.top - ob.bottom) / ob.bottom * 100).toFixed(3)}%
║
║ ⏰ 时间信息
║   ├─ 形成时间: ${formatTime(ob.startTime)}
║   ├─ 确认时间: ${formatTime(ob.confirmationTime)}
║   ├─ 交易时段: ${sessionInfo.emoji} ${sessionInfo.session}
║   ├─ 时段描述: ${sessionInfo.description}
║   └─ 时段可靠性: ${sessionInfo.reliable ? "✅ 高流动性" : "⚠️ 低流动性"}
║
║ 📊 成交量分析 ${parseFloat(ob.volumeRatio) >= 1.2 ? "✅ 已通过" : "❌ 未通过"}
║   ├─ 突破成交量: ${ob.breakoutVolume.toFixed(0)}
║   ├─ SMA20基准: ${ob.volumeSMA20.toFixed(0)}
║   ├─ 成交量比率: ${ob.volumeRatio}x ${parseFloat(ob.volumeRatio) >= 1.2 ? "✅ (>1.2)" : "❌ (<1.2)"}
║   ├─ OB总成交量: ${ob.obVolume.toFixed(0)}
║   ├─ 高量部分: ${ob.obHighVolume.toFixed(0)}
║   └─ 低量部分: ${ob.obLowVolume.toFixed(0)}
║
║ ⚖️ 平衡度评估 ${ob.balancePercent >= 20 && ob.balancePercent <= 80 ? "✅ 已通过" : "❌ 未通过"}
║   ├─ 平衡度: ${ob.balancePercent}% ${ob.balanceQuality}
║   ├─ 有效范围: 20%-80% ${ob.balancePercent >= 20 && ob.balancePercent <= 80 ? "✅" : "❌"}
║   └─ 平衡评价: ${
        ob.balancePercent >= 60 && ob.balancePercent <= 80 ? "理想的买卖平衡" :
        ob.balancePercent >= 40 && ob.balancePercent < 60 ? "较好的买卖平衡" :
        ob.balancePercent >= 20 && ob.balancePercent < 40 ? "一般的买卖平衡" :
        "买卖失衡"
      }
║
║ 🕯️ 突破K线形态分析
║   ├─ 形态类型: ${bp.candleEmoji} ${bp.candleType}
║   ├─ K线方向: ${bp.direction} ${bp.directionMatchEmoji}
║   ├─ 方向匹配: ${bp.isDirectionMatched ? "✅ 与OB类型一致" : "⚠️ 与OB类型不一致"}
║   ├─ 突破强度: ${bp.breakoutEmoji} ${bp.breakoutStrength} (得分: ${bp.strengthScore}/100)
║   ├─ 价格变动: ${bp.priceChangePercent}%
║   ├─ 实体占比: ${bp.bodyPercent}% (实体大小: ${bp.body})
║   ├─ 上影线: ${bp.upperWickPercent}% (长度: ${bp.upperWick})
║   ├─ 下影线: ${bp.lowerWickPercent}% (长度: ${bp.lowerWick})
║   ├─ 总波动: ${bp.totalRange}
║   ├─ 形态描述: ${bp.description}
║   └─ 交易建议: ${bp.recommendation}
║
║ 🎯 状态信息
║   ├─ Breaker: ${ob.breaker ? "🟡 已触及" : "🟢 未触及"}
║   ├─ 有效性: ${ob.isValid ? "✅ 有效" : "❌ 已失效"}
${ob.breaker ? `║   └─ 触及时间: ${formatTime(ob.breakTime)}` : "║   └─ 区域完整性: 保持完好"}
║
║ 💡 综合评分
║   ├─ K线强度: ${bp.strengthScore}/100 ${bp.breakoutEmoji}
║   ├─ 成交量: ${parseFloat(ob.volumeRatio) >= 1.2 ? "✅" : "❌"} (${ob.volumeRatio}x)
║   ├─ 平衡度: ${ob.balancePercent >= 20 && ob.balancePercent <= 80 ? "✅" : "❌"} (${ob.balancePercent}%)
║   ├─ 时段: ${sessionInfo.reliable ? "✅" : "⚠️"} (${sessionInfo.session})
║   └─ 整体评价: ${
        bp.strengthScore >= 80 && parseFloat(ob.volumeRatio) >= 1.2 && sessionInfo.reliable 
          ? "🔥🔥🔥 极强信号" :
        bp.strengthScore >= 60 && parseFloat(ob.volumeRatio) >= 1.2 
          ? "🔥🔥 强信号" :
        bp.strengthScore >= 40 
          ? "🔥 中等信号" :
          "⚠️ 弱信号"
      }
╚═══════════════════════════════════════════════════════════════════════════════
`;
}

function logAllOBs(allZonesData, context) {
  context.log("\n" + "█".repeat(80));
  context.log("█" + " ".repeat(78) + "█");
  context.log("█" + " ".repeat(20) + "📊 所有检测到的 ORDER BLOCKS 详细信息" + " ".repeat(20) + "█");
  context.log("█" + " ".repeat(78) + "█");
  context.log("█".repeat(80));
  
  let totalBullish = 0;
  let totalBearish = 0;
  let totalHighQuality = 0;
  let totalMediumQuality = 0;
  let totalLowQuality = 0;
  
  for (const { symbol, timeframe, zones } of allZonesData) {
    const bullishCount = zones.bullishOBs.length;
    const bearishCount = zones.bearishOBs.length;
    
    totalBullish += bullishCount;
    totalBearish += bearishCount;
    
    if (bullishCount === 0 && bearishCount === 0) {
      context.log(`\n${symbol} - ${timeframe}: ⚠️ 未检测到Order Blocks`);
      continue;
    }
    
    context.log(`\n${"═".repeat(80)}`);
    context.log(`║ 🎯 交易对: ${symbol} - 时间周期: ${timeframe}`);
    context.log(`║    🟢 看涨OB: ${bullishCount} 个 | 🔴 看跌OB: ${bearishCount} 个`);
    context.log(`${"═".repeat(80)}`);
    
    // 显示所有看涨OB
    if (bullishCount > 0) {
      context.log(`\n${"─".repeat(80)}`);
      context.log(`🟢 BULLISH ORDER BLOCKS (看涨支撑区) - 共 ${bullishCount} 个`);
      context.log(`${"─".repeat(80)}`);
      
      zones.bullishOBs.forEach((ob, idx) => {
        context.log(formatOBDetails(ob, idx, symbol, timeframe));
        
        const score = ob.breakoutPattern.strengthScore;
        if (score >= 80) totalHighQuality++;
        else if (score >= 60) totalMediumQuality++;
        else totalLowQuality++;
      });
    }
    
    // 显示所有看跌OB
    if (bearishCount > 0) {
      context.log(`\n${"─".repeat(80)}`);
      context.log(`🔴 BEARISH ORDER BLOCKS (看跌阻力区) - 共 ${bearishCount} 个`);
      context.log(`${"─".repeat(80)}`);
      
      zones.bearishOBs.forEach((ob, idx) => {
        context.log(formatOBDetails(ob, idx, symbol, timeframe));
        
        const score = ob.breakoutPattern.strengthScore;
        if (score >= 80) totalHighQuality++;
        else if (score >= 60) totalMediumQuality++;
        else totalLowQuality++;
      });
    }
  }
  
  // 总计统计
  context.log("\n" + "█".repeat(80));
  context.log("█" + " ".repeat(78) + "█");
  context.log("█" + " ".repeat(30) + "📈 总计统计报告" + " ".repeat(30) + "█");
  context.log("█" + " ".repeat(78) + "█");
  context.log("█".repeat(80));
  context.log(`
║ 🟢 总看涨OB数量: ${totalBullish}
║ 🔴 总看跌OB数量: ${totalBearish}
║ 📊 Order Blocks总计: ${totalBullish + totalBearish}
║
║ 📊 质量分布:
║   ├─ 🔥🔥🔥 高质量OB (≥80分): ${totalHighQuality}
║   ├─ 🔥🔥 中等质量OB (60-79分): ${totalMediumQuality}
║   └─ 🔥 低质量OB (<60分): ${totalLowQuality}
║
║ 💡 建议关注: ${totalHighQuality} 个高质量Order Blocks
`);
  context.log("█".repeat(80) + "\n");
}

// ============================================================================
// --- Order Block 技术指标计算 ---
// ============================================================================

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

function calculateBalancePercentage(obHighVolume, obLowVolume) {
  const maxVol = Math.max(obHighVolume, obLowVolume);
  const minVol = Math.min(obHighVolume, obLowVolume);
  
  if (maxVol === 0) return 0;
  
  return Math.round((minVol / maxVol) * 100);
}

function evaluateBalanceQuality(balance) {
  if (balance >= 60 && balance <= 80) return "🟢 优秀";
  if (balance >= 40 && balance < 60) return "🟡 良好";
  if (balance >= 20 && balance < 40) return "🟠 一般";
  return "🔴 较差";
}

// ============================================================================
// --- Order Block 识别 ---
// ============================================================================

function findOrderBlocksPineScriptLogic(
  klines,
  swingLength = 10,
  obEndMethod = "Wick",
  maxATRMult = 3.5,
  volumeMultiplier = 1.2,
  volumeSMAPeriod = 20,
  minBalancePercent = 20,
  maxBalancePercent = 80
) {
  const bullishOBs = [];
  const bearishOBs = [];
  
  const stats = {
    totalBullishSignals: 0,
    totalBearishSignals: 0,
    bullishRejectedByVolume: 0,
    bearishRejectedByVolume: 0,
    bullishRejectedByBalance: 0,
    bearishRejectedByBalance: 0,
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
    
    if (klines[refIndex].high > upper) {
      if (swingType !== 0) {
        lastSwingHigh = { index: refIndex, high: klines[refIndex].high, crossed: false };
      }
      swingType = 0;
    }
    
    if (klines[refIndex].low < lower) {
      if (swingType !== 1) {
        lastSwingLow = { index: refIndex, low: klines[refIndex].low, crossed: false };
      }
      swingType = 1;
    }
    
    const currentCandle = klines[barIndex];
    
    if (lastSwingHigh && !lastSwingHigh.crossed && currentCandle.close > lastSwingHigh.high) {
      lastSwingHigh.crossed = true;
      stats.totalBullishSignals++;
      
      const volumeSMA20 = calculateVolumeSMA(klines, barIndex, volumeSMAPeriod);
      const volumeThreshold = volumeSMA20 * volumeMultiplier;
      
      if (currentCandle.volume <= volumeThreshold) {
        stats.bullishRejectedByVolume++;
        continue;
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
      
      const balancePercent = calculateBalancePercentage(obHighVolume, obLowVolume);
      
      if (balancePercent < minBalancePercent || balancePercent > maxBalancePercent) {
        stats.bullishRejectedByBalance++;
        continue;
      }
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        const breakoutPattern = analyzeBreakoutCandlePattern(currentCandle, "Support");
        
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
          balancePercent,
          balanceQuality: evaluateBalanceQuality(balancePercent),
          breakoutPattern,
          isValid: true,
          breaker: false,
          breakTime: null,
          type: "Support"
        });
      }
    }
    
    if (lastSwingLow && !lastSwingLow.crossed && currentCandle.close < lastSwingLow.low) {
      lastSwingLow.crossed = true;
      stats.totalBearishSignals++;
      
      const volumeSMA20 = calculateVolumeSMA(klines, barIndex, volumeSMAPeriod);
      const volumeThreshold = volumeSMA20 * volumeMultiplier;
      
      if (currentCandle.volume <= volumeThreshold) {
        stats.bearishRejectedByVolume++;
        continue;
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
      
      const balancePercent = calculateBalancePercentage(obHighVolume, obLowVolume);
      
      if (balancePercent < minBalancePercent || balancePercent > maxBalancePercent) {
        stats.bearishRejectedByBalance++;
        continue;
      }
      
      const obSize = Math.abs(boxTop - boxBtm);
      
      if (obSize <= atr * maxATRMult) {
        const breakoutPattern = analyzeBreakoutCandlePattern(currentCandle, "Resistance");
        
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
          balancePercent,
          balanceQuality: evaluateBalanceQuality(balancePercent),
          breakoutPattern,
          isValid: true,
          breaker: false,
          breakTime: null,
          type: "Resistance"
        });
      }
    }
    
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
    stats
  };
}

// ============================================================================
// --- 潜在新zone预检测 ---
// ============================================================================

function detectPotentialNewZones(allZonesData, context) {
  const now = new Date();
  const potentialNewZones = [];
  
  context.log("\n🔍 检测潜在新zones (使用智能时间窗口)...");
  
  for (const { symbol, timeframe, zones } of allZonesData) {
    const windowMinutes = RUNTIME_CONFIG.TIMEFRAME_WINDOWS[timeframe] || 90;
    const timeThreshold = new Date(now.getTime() - windowMinutes * 60 * 1000);
    
    const allZones = [...zones.bullishOBs, ...zones.bearishOBs];
    
    const recentZones = allZones.filter(zone => 
      zone.confirmationTime >= timeThreshold
    );
    
    if (recentZones.length > 0) {
      context.log(
        `  🆕 ${symbol} ${timeframe}: 发现 ${recentZones.length} 个潜在新zones\n` +
        `      检测窗口: ${windowMinutes} 分钟 (${(windowMinutes/60).toFixed(1)} 小时)\n` +
        `      时间阈值: ${timeThreshold.toISOString()}`
      );
      
      for (const zone of recentZones) {
        const zoneIdentifier = `${symbol}-${timeframe}-${zone.startTime.getTime()}-${zone.type}`;
        potentialNewZones.push({
          identifier: zoneIdentifier,
          symbol,
          timeframe,
          zone,
          windowUsed: windowMinutes
        });
      }
    } else {
      context.log(
        `  ⏭️ ${symbol} ${timeframe}: 在最近 ${windowMinutes} 分钟内无新zones`
      );
    }
  }
  
  return potentialNewZones;
}

// ============================================================================
// --- Storage缓存系统 ---
// ============================================================================

async function checkStorageBucketExists(storage, context) {
  const bucketId = RUNTIME_CONFIG.STORAGE_CONFIG.BUCKET_ID;
  
  try {
    await storage.getBucket(bucketId);
    context.log(`✅ Storage Bucket "${bucketId}" 已存在`);
    return true;
  } catch (e) {
    context.error(`❌ Storage Bucket "${bucketId}" 不存在`);
    context.error(`   请在 Appwrite Console → Storage 中手动创建`);
    context.error(`   错误详情: ${e.message}`);
    return false;
  }
}

async function loadZonesFromStorage(storage, context) {
  try {
    const fileBuffer = await storage.getFileDownload(
      RUNTIME_CONFIG.STORAGE_CONFIG.BUCKET_ID,
      RUNTIME_CONFIG.STORAGE_CONFIG.FILE_ID
    );
    
    const jsonString = fileBuffer.toString('utf-8');
    const data = JSON.parse(jsonString);
    
    context.log(
      `✅ 从Storage加载成功:\n` +
      `   记录数量: ${data.zones.length}\n` +
      `   最后更新: ${data.lastUpdated}\n` +
      `   文件版本: ${data.version || 'v1'}`
    );
    
    return new Set(data.zones);
  } catch (e) {
    if (e.message && (e.message.includes('not found') || e.message.includes('File not found'))) {
      context.log("⚠️ Storage文件不存在（首次运行正常），返回空Set");
      return new Set();
    }
    if (e.code === 404 || e.type === 'storage_file_not_found') {
      context.log("⚠️ Storage文件不存在（首次运行正常），返回空Set");
      return new Set();
    }
    context.error("❌ 加载Storage失败:", e.message);
    return new Set();
  }
}

async function saveZonesToStorage(storage, zones, context) {
  try {
    const data = {
      version: "v1.0",
      zones: Array.from(zones),
      lastUpdated: new Date().toISOString(),
      count: zones.size,
      metadata: {
        cleanupDays: RUNTIME_CONFIG.STORAGE_CONFIG.CLEANUP_DAYS,
        generatedBy: "OB-Detector-v4.3"
      }
    };
    
    const jsonString = JSON.stringify(data, null, 2);
    const fileBuffer = Buffer.from(jsonString, 'utf-8');
    const inputFile = InputFile.fromBuffer(
      fileBuffer,
      RUNTIME_CONFIG.STORAGE_CONFIG.FILE_ID
    );
    
    // 先尝试删除旧文件
    try {
      await storage.deleteFile(
        RUNTIME_CONFIG.STORAGE_CONFIG.BUCKET_ID,
        RUNTIME_CONFIG.STORAGE_CONFIG.FILE_ID
      );
      context.log("   🗑️ 删除旧Storage文件");
    } catch (e) {
      // 文件不存在，忽略错误
    }
    
    // 上传新文件
    await storage.createFile(
      RUNTIME_CONFIG.STORAGE_CONFIG.BUCKET_ID,
      RUNTIME_CONFIG.STORAGE_CONFIG.FILE_ID,
      inputFile
    );
    
    context.log(
      `✅ 保存到Storage成功:\n` +
      `   记录数量: ${zones.size}\n` +
      `   文件大小: ${(jsonString.length / 1024).toFixed(2)} KB`
    );
    return true;
  } catch (e) {
    context.error("❌ 保存到Storage失败:", e.message);
    context.error("   错误类型:", e.type);
    context.error("   错误代码:", e.code);
    return false;
  }
}

async function cleanupStorageZones(zones, context) {
  const cutoffTime = Date.now() - RUNTIME_CONFIG.STORAGE_CONFIG.CLEANUP_DAYS * 24 * 60 * 60 * 1000;
  
  const cleanedZones = new Set(
    Array.from(zones).filter(identifier => {
      const parts = identifier.split('-');
      if (parts.length >= 3) {
        const timestamp = parseInt(parts[2]);
        return timestamp > cutoffTime;
      }
      return true;
    })
  );
  
  const removed = zones.size - cleanedZones.size;
  if (removed > 0) {
    context.log(
      `🗑️ 清理Storage数据:\n` +
      `   移除过期记录: ${removed} 条 (>${RUNTIME_CONFIG.STORAGE_CONFIG.CLEANUP_DAYS}天)\n` +
      `   保留记录: ${cleanedZones.size} 条`
    );
  }
  
  return cleanedZones;
}

// ============================================================================
// --- Database备份系统 ---
// ============================================================================

async function loadZonesFromDatabase(databases, DB_ID, COLLECTION_ID, context) {
  try {
    const response = await databases.listDocuments(
      DB_ID,
      COLLECTION_ID,
      [
        Query.limit(500),
        Query.orderDesc('$createdAt')
      ]
    );
    
    context.log(`✅ 从Database加载: ${response.documents.length} 条记录`);
    return new Set(response.documents.map(doc => doc.zoneIdentifier));
  } catch (e) {
    context.error("❌ 从Database加载失败:", e.message);
    return new Set();
  }
}

async function saveZonesToDatabase(databases, DB_ID, COLLECTION_ID, newZoneIdentifiers, context) {
  if (newZoneIdentifiers.length === 0) return 0;
  
  try {
    const promises = newZoneIdentifiers.map(zoneId =>
      databases.createDocument(DB_ID, COLLECTION_ID, ID.unique(), {
        zoneIdentifier: zoneId
      }).catch(e => {
        if (e.code !== 409) {
          context.error(`保存失败: ${zoneId}`, e.message);
        }
        return null;
      })
    );
    
    const results = await Promise.all(promises);
    const savedCount = results.filter(r => r !== null).length;
    
    if (savedCount > 0) {
      context.log(`✅ 保存到Database: ${savedCount} 条`);
    }
    
    return savedCount;
  } catch (e) {
    context.error("❌ 批量保存到Database失败:", e.message);
    return 0;
  }
}

function shouldSyncToDatabase() {
  const now = new Date();
  const hour = now.getUTCHours();
  const minute = now.getUTCMinutes();
  
  const syncHour = RUNTIME_CONFIG.DB_CONFIG.SYNC_HOUR_UTC;
  const syncWindow = RUNTIME_CONFIG.DB_CONFIG.SYNC_WINDOW_MINUTES;
  
  return hour === syncHour && minute < syncWindow;
}

// ============================================================================
// --- 生成通知消息 ---
// ============================================================================

function generateNotificationMessage(symbol, timeframe, zone, CONFIG) {
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

  const sessionInfo = getMarketSession(zone.confirmationTime);
  const reliabilityWarning = !sessionInfo.reliable 
    ? `\n⚠️ *注意: ${sessionInfo.description}，信号可靠性较低*` 
    : '';

  const bp = zone.breakoutPattern;
  const patternWarning = !bp.isDirectionMatched 
    ? `\n⚠️ *警告: 突破K线方向与OB类型不匹配，谨慎对待*`
    : '';

  const message = `*🔔 新 Order Block 区域警报*\n\n` +
    `*交易对:* ${symbol}\n` +
    `*时间周期:* ${timeframe}\n` +
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
    
    `*🕯️ 突破K线形态分析*\n` +
    `• 形态类型: ${bp.candleEmoji} ${bp.candleType}\n` +
    `• K线方向: ${bp.direction} ${bp.directionMatchEmoji}\n` +
    `• 突破强度: ${bp.breakoutEmoji} *${bp.breakoutStrength}* (${bp.strengthScore}/100)\n` +
    `• 价格变动: ${bp.priceChangePercent}%\n` +
    `• 实体占比: ${bp.bodyPercent}% (总波动: ${bp.totalRange})\n` +
    `• 上影线: ${bp.upperWickPercent}%\n` +
    `• 下影线: ${bp.lowerWickPercent}%\n` +
    `• 形态描述: ${bp.description}\n` +
    `• *建议: ${bp.recommendation}*${patternWarning}\n\n` +
    
    `*⏰ 时间与时段信息*\n` +
    `• OB 形成时间: ${formatNZTime(zone.startTime)}\n` +
    `• 突破确认时间: ${formatNZTime(zone.confirmationTime)}\n` +
    `• 确认时段: ${sessionInfo.emoji} *${sessionInfo.session}*\n` +
    `• 时段描述: ${sessionInfo.description}${reliabilityWarning}\n\n` +
    
    `_此区域已通过成交量、平衡度与K线形态三重验证_`;

  const subject = `🔔 ${symbol} ${timeframe} 新${zone.type}区域 [${bp.breakoutStrength}突破] [平衡度${zone.balancePercent}%] [${sessionInfo.session}]`;

  return { message, subject };
}

// ============================================================================
// --- Appwrite Function Entrypoint ---
// ============================================================================
module.exports = async (context) => {
  const executionStart = Date.now();
  context.log("🚀 Function execution started (v4.3 - 完整验证版本)...");
  context.log(`⏰ 执行时间: ${new Date().toISOString()}`);
  context.log(`🔄 执行频率: 每 ${RUNTIME_CONFIG.EXECUTION_INTERVAL_MINUTES} 分钟\n`);

  const CONFIG = {
    SYMBOLS: ["BTCUSDT", "ETHUSDT"],
    TIMEZONES: ["1h", "4h", "1d"],
    SWING_LENGTH: 10,
    OB_END_METHOD: "Wick",
    MAX_ATR_MULT: 3.5,
    KLINE_LIMIT: 1000,
    
    VOLUME_MULTIPLIER: 1.2,
    VOLUME_SMA_PERIOD: 20,
    
    MIN_BALANCE_PERCENT: 20,
    MAX_BALANCE_PERCENT: 80,

    ENABLE_TELEGRAM: true,
    TELEGRAM_BOT_TOKEN: "7607543807:AAFcNXDZE_ctPhTQVc60vnX69o0zPjzsLb0",
    TELEGRAM_CHAT_ID: "7510264240",

    ENABLE_EMAIL: true,
    EMAIL_RECIPIENT: "jiaxu09@gmail.com",
    EMAIL_CONFIG: {
      service: "gmail",
      auth: { user: "jiaxu99.w@gmail.com", pass: "hqmv qwbm qpik juiq" },
    },
  };

  const client = new Client()
    .setEndpoint('https://syd.cloud.appwrite.io/v1')
    .setProject('68f59e58002322d3d474')
    .setKey('standard_2555e90b24b6442cafa174ecccc387d2668557a61d73186f705f7e65681f9ed2cbbf5a672f55669cb9a549a5a8a282b2f1dd32e3f3a1a818dd06c2ce4e23f72da594fddd5dfcd736f0bb04d1151962a6fb9568a25c700e8d4746eddc96ec2538556dd23e696117ad6ebdbdb05856a5250fb125e03b3484fd6b73e24d245c59e8');

  const storage = new Storage(client);
  const databases = new Databases(client);
  const DB_ID = "68f5a3fa001774a5ab3d";
  const COLLECTION_ID = "seen_zones";

  // 检查Storage Bucket是否存在
  const bucketExists = await checkStorageBucketExists(storage, context);

  // ============================================================================
  // 步骤1：分析所有symbols，收集所有OB数据
  // ============================================================================
  
  context.log("\n📊 Step 1: 分析所有交易对和时间周期...\n");
  const allZonesData = [];
  
  for (const symbol of CONFIG.SYMBOLS) {
    context.log(`--- 分析 ${symbol} ---`);
    
    for (const tf of CONFIG.TIMEZONES) {
      const klines = await getKlines(symbol, tf, CONFIG.KLINE_LIMIT, context);
      if (!klines || klines.length <= CONFIG.SWING_LENGTH) {
        context.log(`⚠️ ${symbol} ${tf} 数据不足，跳过`);
        continue;
      }

      const result = findOrderBlocksPineScriptLogic(
        klines,
        CONFIG.SWING_LENGTH,
        CONFIG.OB_END_METHOD,
        CONFIG.MAX_ATR_MULT,
        CONFIG.VOLUME_MULTIPLIER,
        CONFIG.VOLUME_SMA_PERIOD,
        CONFIG.MIN_BALANCE_PERCENT,
        CONFIG.MAX_BALANCE_PERCENT
      );
      
      context.log(
        `  ${symbol} ${tf}: ` +
        `🟢 ${result.bullishOBs.length} 看涨 | ` +
        `🔴 ${result.bearishOBs.length} 看跌 ` +
        `(已过滤: 成交量 ${result.stats.bullishRejectedByVolume + result.stats.bearishRejectedByVolume}, ` +
        `平衡度 ${result.stats.bullishRejectedByBalance + result.stats.bearishRejectedByBalance})`
      );
      
      allZonesData.push({
        symbol,
        timeframe: tf,
        zones: result
      });
    }
    context.log('');
  }

  // 显示所有检测到的Order Blocks详细信息
  logAllOBs(allZonesData, context);

  // ============================================================================
  // 步骤2：预检测潜在新zones
  // ============================================================================
  
  const potentialNewZones = detectPotentialNewZones(allZonesData, context);
  const needDailySync = shouldSyncToDatabase();
  
  if (potentialNewZones.length === 0 && !needDailySync) {
    context.log("\n✅ 未检测到潜在新zones，且不在同步时间窗口");
    context.log("⚡ 跳过所有存储操作 - 0次Storage/Database操作！");
    
    const executionTime = ((Date.now() - executionStart) / 1000).toFixed(2);
    
    return context.res.json({
      success: true,
      new_zones_found: 0,
      storage_reads: 0,
      storage_writes: 0,
      database_reads: 0,
      database_writes: 0,
      execution_time_seconds: executionTime,
      optimization_triggered: true,
      message: "无新zones且不在同步窗口 - 已跳过所有存储操作",
      timestamp: new Date().toISOString()
    });
  }

  context.log(`\n🆕 发现 ${potentialNewZones.length} 个潜在新zones - 进行存储比对...\n`);

  // ============================================================================
  // 步骤3：从Storage/Database加载已存在的zones
  // ============================================================================
  
  let storageReads = 0;
  let storageWrites = 0;
  let databaseReads = 0;
  let databaseWrites = 0;
  
  let previousZones = new Set();
  let useDatabase = false;
  
  context.log("💾 Step 3: 从Storage加载已存在的zones...");
  
  if (bucketExists) {
    previousZones = await loadZonesFromStorage(storage, context);
    storageReads++;
    
    // 如果Storage返回空Set，尝试从Database加载（防止Storage故障）
    if (previousZones.size === 0 && RUNTIME_CONFIG.DB_CONFIG.USE_DB_FALLBACK) {
      context.log("⚠️ Storage为空，尝试从Database加载...");
      previousZones = await loadZonesFromDatabase(databases, DB_ID, COLLECTION_ID, context);
      databaseReads++;
      useDatabase = true;
    }
  } else {
    context.log("⚠️ Storage不可用，使用Database");
    previousZones = await loadZonesFromDatabase(databases, DB_ID, COLLECTION_ID, context);
    databaseReads++;
    useDatabase = true;
  }

  // ============================================================================
  // 步骤4：精确比对，找出真正的新zones
  // ============================================================================
  
  context.log("\n🔍 Step 4: 比对并确认新zones...");
  const confirmedNewZones = [];
  const allNewNotifications = [];
  
  for (const potentialZone of potentialNewZones) {
    if (!previousZones.has(potentialZone.identifier)) {
      context.log(`  ✅ 确认新zone: ${potentialZone.identifier}`);
      context.log(`     使用的检测窗口: ${potentialZone.windowUsed} 分钟`);
      confirmedNewZones.push(potentialZone.identifier);
      
      // 添加到内存Set
      previousZones.add(potentialZone.identifier);
      
      const { message, subject } = generateNotificationMessage(
        potentialZone.symbol,
        potentialZone.timeframe,
        potentialZone.zone,
        CONFIG
      );
      
      allNewNotifications.push({ message, subject });
    } else {
      context.log(`  ⏭️ Zone已存在: ${potentialZone.identifier}`);
    }
  }

  // ============================================================================
  // 步骤5：保存新zones到Storage并清理旧数据
  // ============================================================================
  
  if (confirmedNewZones.length > 0) {
    context.log(`\n💾 Step 5: 保存 ${confirmedNewZones.length} 个新zones到Storage...`);
    
    // 清理旧数据
    previousZones = await cleanupStorageZones(previousZones, context);
    
    // 尝试保存到Storage
    let saved = false;
    
    if (bucketExists) {
      saved = await saveZonesToStorage(storage, previousZones, context);
      if (saved) {
        storageWrites++;
      }
    }
    
    // 如果Storage保存失败或使用Database模式，保存到Database
    if (!saved || useDatabase) {
      context.log("   → 保存到Database作为备份...");
      const count = await saveZonesToDatabase(databases, DB_ID, COLLECTION_ID, confirmedNewZones, context);
      databaseWrites += count;
    }
  } else {
    context.log("\n✅ 无新zones需要保存");
  }

  // ============================================================================
  // 步骤6：发送通知
  // ============================================================================
  
  if (allNewNotifications.length > 0) {
    context.log(`\n✉️ Step 6: 发送 ${allNewNotifications.length} 条通知...`);
    for (const n of allNewNotifications) {
      await sendTelegramNotification(CONFIG, n.message, context);
      await sendEmailNotification(CONFIG, n.subject, n.message, context);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  } else {
    context.log("\n✅ 无需发送通知");
  }

  // ============================================================================
  // 步骤7：返回执行统计
  // ============================================================================
  
  const executionTime = ((Date.now() - executionStart) / 1000).toFixed(2);
  
  context.log("\n" + "=".repeat(80));
  context.log("🎉 Function执行完成!");
  context.log(`⏱️ 总执行时间: ${executionTime}秒`);
  context.log(`💾 Storage操作: ${storageReads}次读取 + ${storageWrites}次写入`);
  context.log(`📊 Database操作: ${databaseReads}次读取 + ${databaseWrites}次写入`);
  context.log(`🆕 新zones数量: ${allNewNotifications.length}`);
  context.log(`🔄 Storage状态: ${bucketExists ? '✅ 可用' : '❌ 不可用'}`);
  context.log("=".repeat(80) + "\n");
  
  return context.res.json({
    success: true,
    new_zones_found: allNewNotifications.length,
    potential_zones_detected: potentialNewZones.length,
    confirmed_new_zones: confirmedNewZones.length,
    storage_reads: storageReads,
    storage_writes: storageWrites,
    database_reads: databaseReads,
    database_writes: databaseWrites,
    execution_time_seconds: executionTime,
    storage_available: bucketExists,
    storage_used: !useDatabase,
    timestamp: new Date().toISOString()
  });
};