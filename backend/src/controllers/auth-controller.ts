import bcrypt from "bcryptjs";
import type { Request, Response } from "express";
import { prisma } from "../db.js";
import { authSchema } from "../types/auth-schema.js";
import { createToken } from "../utils/auth.js";
import { sendValidationError } from "../utils/validation.js";

export async function signup(req: Request, res: Response): Promise<void> {
  const parsedBody = authSchema.safeParse(req.body);
  if (!parsedBody.success) {
    sendValidationError(res, parsedBody.error);
    return;
  }

  const { username, password } = parsedBody.data;
  const hashedPassword = await bcrypt.hash(password, 10);

  try {
    const user = await prisma.user.create({
      data: {
        username,
        password: hashedPassword,
      },
    });

    res.status(201).json({
      token: createToken({ userId: user.id }),
      userId: user.id,
      username: user.username,
    });
  } catch {
    res.status(409).json({ error: "username already exists" });
  }
}

export async function signin(req: Request, res: Response): Promise<void> {
  const parsedBody = authSchema.safeParse(req.body);
  if (!parsedBody.success) return sendValidationError(res, parsedBody.error);
  const { username, password } = parsedBody.data;
  try {
    const loginUser = await prisma.user.findUnique({ where: { username: username } });
    if (!loginUser) {
      res.status(404).json({ error: "user not found" });
      return;
    }
    const isAuthenticated = await bcrypt.compare(password, loginUser!.password);
    if (!isAuthenticated) {
      res.status(401).json({ error: "invalid credentials" });
      return;
    }
    const token = createToken({ userId: loginUser!.id })
    res.status(200).json({ token });
  } catch {
    res.status(500).json({ error: "internal server error" });
  }
}
