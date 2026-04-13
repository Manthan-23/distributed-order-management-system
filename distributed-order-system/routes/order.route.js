import { Router } from "express";
import * as controller from "../controllers/appController.js";


const router = Router();


router.route("/create-order").post(controller.createOrder);

router.route("/orders/:id").get(controller.getOrder);



export default router;